// Monitor SQL: executa periodicamente uma consulta no PostgreSQL, compara o
// resultado com o último valor armazenado em disco e emite uma notificação
// na área de trabalho quando o valor muda.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"gopkg.in/yaml.v3"
)

const (
	defaultConfigFile = "config.yaml"
	dbTimeout         = 10 * time.Second
	notifyTimeout     = 5 * time.Second
	notifyTitle       = "SQL Monitor"
)

// ------------------------------------------------------------
// Configuração
// ------------------------------------------------------------

type Config struct {
	Database DatabaseConfig `yaml:"database"`
	Monitor  MonitorConfig  `yaml:"monitor"`
}

type DatabaseConfig struct {
	Host    string `yaml:"host"`
	Port    int    `yaml:"port"`
	Name    string `yaml:"name"`
	User    string `yaml:"user"`
	SSLMode string `yaml:"sslmode"`
}

type MonitorConfig struct {
	Interval  int    `yaml:"interval"` // segundos
	StateFile string `yaml:"state_file"`
	LogFile   string `yaml:"log_file"`
	Query     string `yaml:"query"`
}

func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("yaml inválido: %w", err)
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func (c *Config) validate() error {
	var problems []string

	if c.Database.Host == "" {
		problems = append(problems, "database.host é obrigatório")
	}
	if c.Database.Port <= 0 {
		problems = append(problems, "database.port deve ser maior que zero")
	}
	if c.Database.Name == "" {
		problems = append(problems, "database.name é obrigatório")
	}
	if c.Database.User == "" {
		problems = append(problems, "database.user é obrigatório")
	}
	if c.Monitor.Interval <= 0 {
		problems = append(problems, "monitor.interval deve ser maior que zero")
	}
	if c.Monitor.StateFile == "" {
		problems = append(problems, "monitor.state_file é obrigatório")
	}
	if c.Monitor.LogFile == "" {
		problems = append(problems, "monitor.log_file é obrigatório")
	}
	if strings.TrimSpace(c.Monitor.Query) == "" {
		problems = append(problems, "monitor.query é obrigatório")
	}

	if len(problems) > 0 {
		return fmt.Errorf("configuração inválida: %s", strings.Join(problems, "; "))
	}
	return nil
}

func (c *DatabaseConfig) dsn() string {
	return fmt.Sprintf(
		"postgres://%s@%s:%d/%s?sslmode=%s",
		c.User, c.Host, c.Port, c.Name, c.SSLMode,
	)
}

func (c *MonitorConfig) interval() time.Duration {
	return time.Duration(c.Interval) * time.Second
}

// ------------------------------------------------------------
// Estado persistido (último valor observado)
// ------------------------------------------------------------

type stateStore struct {
	path string
}

// load devolve o último valor salvo. found é false na primeira execução,
// quando o arquivo ainda não existe.
//
// Se o caminho for um diretório (por exemplo, criado indevidamente por uma
// versão anterior ou à mão), um diretório vazio é removido e a execução é
// tratada como a primeira; um diretório com conteúdo gera um erro explicativo.
func (s stateStore) load() (value string, found bool, err error) {
	info, err := os.Stat(s.path)
	if errors.Is(err, fs.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	if info.IsDir() {
		// os.Remove só remove diretórios vazios; um diretório com conteúdo
		// não é apagado silenciosamente.
		if rmErr := os.Remove(s.path); rmErr != nil {
			return "", false, fmt.Errorf(
				"state_file %q é um diretório e não está vazio; remova-o ou ajuste monitor.state_file: %w",
				s.path, rmErr,
			)
		}
		return "", false, nil
	}

	data, err := os.ReadFile(s.path)
	if err != nil {
		return "", false, err
	}
	return strings.TrimSpace(string(data)), true, nil
}

func (s stateStore) save(value string) error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(s.path, []byte(value+"\n"), 0o644)
}

// ------------------------------------------------------------
// Notificação na área de trabalho
// ------------------------------------------------------------

type notifyFunc func(title, message string)

func desktopNotify(title, message string) {
	ctx, cancel := context.WithTimeout(context.Background(), notifyTimeout)
	defer cancel()

	_ = exec.CommandContext(ctx, "notify-send", "-u", "critical", title, message).Run()
}

// ------------------------------------------------------------
// Monitor
// ------------------------------------------------------------

type Monitor struct {
	cfg    *Config
	state  stateStore
	logger *log.Logger
	notify notifyFunc
	conn   *pgx.Conn
}

func newMonitor(cfg *Config, logger *log.Logger, notify notifyFunc) *Monitor {
	return &Monitor{
		cfg:    cfg,
		state:  stateStore{path: cfg.Monitor.StateFile},
		logger: logger,
		notify: notify,
	}
}

// Run executa o loop de monitoramento até que ctx seja cancelado.
func (m *Monitor) Run(ctx context.Context) {
	defer m.closeConn()

	ticker := time.NewTicker(m.cfg.Monitor.interval())
	defer ticker.Stop()

	for {
		m.tick(ctx)

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// tick faz uma rodada completa: garante conexão, consulta, compara e registra.
func (m *Monitor) tick(ctx context.Context) {
	if err := m.ensureConn(ctx); err != nil {
		m.logger.Println("Erro ao conectar ao PostgreSQL:", err)
		m.notify(notifyTitle, "Erro ao conectar ao PostgreSQL")
		return
	}

	current, err := m.queryValue(ctx)
	if err != nil {
		m.logger.Println("Erro ao executar SQL:", err)
		m.notify(notifyTitle, "Erro ao executar consulta SQL")
		return
	}

	if err := m.compare(current); err != nil {
		m.logger.Println("Erro ao verificar valor:", err)
	}
}

// ensureConn abre a conexão na primeira rodada e reabre se ela tiver caído.
func (m *Monitor) ensureConn(ctx context.Context) error {
	if m.conn != nil && !m.conn.IsClosed() {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	conn, err := pgx.Connect(ctx, m.cfg.Database.dsn())
	if err != nil {
		return err
	}

	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close(context.Background())
		return err
	}

	m.conn = conn
	m.logger.Println("Conexão com PostgreSQL estabelecida")
	return nil
}

func (m *Monitor) closeConn() {
	if m.conn == nil {
		return
	}
	_ = m.conn.Close(context.Background())
	m.conn = nil
}

// queryValue executa a consulta e devolve a primeira coluna da primeira linha
// como texto.
func (m *Monitor) queryValue(ctx context.Context) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	var value any
	if err := m.conn.QueryRow(ctx, m.cfg.Monitor.Query).Scan(&value); err != nil {
		return "", err
	}
	return fmt.Sprintf("%v", value), nil
}

// compare confronta o valor atual com o último salvo, notifica em caso de
// alteração e atualiza o estado.
func (m *Monitor) compare(current string) error {
	previous, found, err := m.state.load()
	if err != nil {
		return err
	}

	if !found {
		if err := m.state.save(current); err != nil {
			return err
		}
		m.logger.Printf("Primeiro resultado armazenado: %s", current)
		return nil
	}

	// Sem alteração: nada é gravado no log.
	if current == previous {
		return nil
	}

	m.logger.Println("ALTERAÇÃO DETECTADA!")
	m.logger.Printf("Anterior: %s", previous)
	m.logger.Printf("Atual:    %s", current)

	m.notify(notifyTitle, fmt.Sprintf(
		"Valor alterado!\nAnterior: %s\nAtual: %s",
		previous, current,
	))

	return m.state.save(current)
}

// ------------------------------------------------------------
// Main
// ------------------------------------------------------------

func main() {
	configPath := flag.String("config", defaultConfigFile, "caminho do arquivo de configuração")
	flag.Parse()

	cfg, err := loadConfig(*configPath)
	if err != nil {
		log.Fatalf("Erro ao carregar configuração: %v", err)
	}

	logFile, err := os.OpenFile(cfg.Monitor.LogFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		log.Fatalf("Erro ao abrir log: %v", err)
	}
	defer logFile.Close()

	logger := log.New(logFile, "", 0)
	logger.Println("SQL Monitor iniciado")
	logger.Printf("Banco: %s:%d/%s", cfg.Database.Host, cfg.Database.Port, cfg.Database.Name)
	logger.Printf("Intervalo: %ds", cfg.Monitor.Interval)
	logger.Println("--------------------------------------------------------")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	newMonitor(cfg, logger, desktopNotify).Run(ctx)

	logger.Println("SQL Monitor encerrado")
}
