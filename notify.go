package main

import (
	"context"
	"os/exec"
	"time"
)

const notifyTimeout = 5 * time.Second

// notifyFunc envia uma notificação ao usuário. É injetada no Monitor para
// permitir substituição em testes.
type notifyFunc func(title, message string)

// desktopNotify usa notify-send para exibir um aviso na área de trabalho.
// Falhas são ignoradas: a notificação é um complemento ao log, não a fonte
// de verdade.
func desktopNotify(title, message string) {
	ctx, cancel := context.WithTimeout(context.Background(), notifyTimeout)
	defer cancel()

	_ = exec.CommandContext(ctx, "notify-send", "-u", "critical", title, message).Run()
}
