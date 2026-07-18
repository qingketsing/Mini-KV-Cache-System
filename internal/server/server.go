package server

import (
	"fmt"

	"github.com/qingketsing/Mini-KV-Cache-System/internal/config"
)

// Server owns the lifecycle of a MiniKV node.
type Server struct {
	config config.Config
}

// New creates a server with the provided configuration.
func New(cfg config.Config) *Server {
	return &Server{config: cfg}
}

// Run starts the server. Networking is intentionally left for the next step.
func (s *Server) Run() error {
	fmt.Printf("MiniKV node %s configured on %s\n", s.config.NodeID, s.config.ListenAddr)
	return nil
}
