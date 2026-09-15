package main

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// stateStore persiste em disco o último valor observado pela consulta.
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
