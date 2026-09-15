package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const defaultConfigFile = "config.yaml"

// Config é a raiz do arquivo YAML de configuração.
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

// loadConfig lê e valida o arquivo YAML em path.
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

// dsn monta a URL de conexão no formato aceito pelo pgx.
func (c *DatabaseConfig) dsn() string {
	return fmt.Sprintf(
		"postgres://%s@%s:%d/%s?sslmode=%s",
		c.User, c.Host, c.Port, c.Name, c.SSLMode,
	)
}

func (c *MonitorConfig) interval() time.Duration {
	return time.Duration(c.Interval) * time.Second
}
