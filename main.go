// Monitor SQL: executa periodicamente uma consulta no PostgreSQL, compara o
// resultado com o último valor armazenado em disco e emite uma notificação
// na área de trabalho quando o valor muda.
//
// Organização dos arquivos:
//
//	config.go  - leitura e validação do config.yaml
//	state.go   - persistência do último valor observado
//	notify.go  - notificação na área de trabalho
//	db.go      - acesso ao PostgreSQL (valueSource)
//	monitor.go - loop de monitoramento e comparação
//	main.go    - montagem e ciclo de vida do processo
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
)

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

	monitor := newMonitor(
		cfg.Monitor.interval(),
		newPGSource(cfg.Database, cfg.Monitor.Query),
		stateStore{path: cfg.Monitor.StateFile},
		logger,
		desktopNotify,
	)
	monitor.Run(ctx)

	logger.Println("SQL Monitor encerrado")
}
