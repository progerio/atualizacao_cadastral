package main

import (
	"context"
	"fmt"
	"log"
	"time"
)

const notifyTitle = "SQL Monitor"

// Monitor executa o ciclo periódico: obtém o valor da fonte, compara com o
// último valor persistido, registra e notifica alterações.
type Monitor struct {
	interval time.Duration
	source   valueSource
	state    stateStore
	logger   *log.Logger
	notify   notifyFunc
}

func newMonitor(interval time.Duration, source valueSource, state stateStore, logger *log.Logger, notify notifyFunc) *Monitor {
	return &Monitor{
		interval: interval,
		source:   source,
		state:    state,
		logger:   logger,
		notify:   notify,
	}
}

// Run executa o loop de monitoramento até que ctx seja cancelado.
func (m *Monitor) Run(ctx context.Context) {
	defer m.source.Close()

	ticker := time.NewTicker(m.interval)
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
	opened, err := m.source.Connect(ctx)
	if err != nil {
		m.logger.Println("Erro ao conectar ao PostgreSQL:", err)
		m.notify(notifyTitle, "Erro ao conectar ao PostgreSQL")
		return
	}
	if opened {
		m.logger.Println("Conexão com PostgreSQL estabelecida")
	}

	current, err := m.source.Value(ctx)
	if err != nil {
		m.logger.Println("Erro ao executar SQL:", err)
		m.notify(notifyTitle, "Erro ao executar consulta SQL")
		return
	}

	if err := m.compare(current); err != nil {
		m.logger.Println("Erro ao verificar valor:", err)
	}
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
