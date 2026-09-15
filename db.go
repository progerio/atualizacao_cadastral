package main

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

const dbTimeout = 10 * time.Second

// valueSource fornece o valor monitorado a cada rodada. O Monitor depende
// apenas desta interface, não do PostgreSQL diretamente.
type valueSource interface {
	// Connect garante que a fonte está pronta para consulta, abrindo ou
	// reabrindo a conexão quando necessário. opened é true quando uma
	// conexão nova foi estabelecida nesta chamada.
	Connect(ctx context.Context) (opened bool, err error)
	// Value executa a consulta e devolve o resultado como texto.
	Value(ctx context.Context) (string, error)
	Close()
}

// pgSource implementa valueSource sobre uma conexão pgx.
type pgSource struct {
	dsn   string
	query string
	conn  *pgx.Conn
}

func newPGSource(db DatabaseConfig, query string) *pgSource {
	return &pgSource{dsn: db.dsn(), query: query}
}

// Connect abre a conexão na primeira chamada e reabre se ela tiver caído.
func (s *pgSource) Connect(ctx context.Context) (bool, error) {
	if s.conn != nil && !s.conn.IsClosed() {
		return false, nil
	}

	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	conn, err := pgx.Connect(ctx, s.dsn)
	if err != nil {
		return false, err
	}

	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close(context.Background())
		return false, err
	}

	s.conn = conn
	return true, nil
}

// Value executa a consulta e devolve a primeira coluna da primeira linha
// como texto.
func (s *pgSource) Value(ctx context.Context) (string, error) {
	if s.conn == nil {
		return "", fmt.Errorf("conexão não estabelecida")
	}

	ctx, cancel := context.WithTimeout(ctx, dbTimeout)
	defer cancel()

	var value any
	if err := s.conn.QueryRow(ctx, s.query).Scan(&value); err != nil {
		return "", err
	}
	return fmt.Sprintf("%v", value), nil
}

func (s *pgSource) Close() {
	if s.conn == nil {
		return
	}
	_ = s.conn.Close(context.Background())
	s.conn = nil
}
