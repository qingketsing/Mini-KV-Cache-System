package main

import (
	"fmt"
	"os"

	"github.com/qingketsing/Mini-KV-Cache-System/internal/config"
	"github.com/qingketsing/Mini-KV-Cache-System/internal/server"
)

func main() {
	cfg := config.Default()
	srv := server.New(cfg)

	if err := srv.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "minikv: %v\n", err)
		os.Exit(1)
	}
}
