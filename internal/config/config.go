package config

// Config contains process-level settings for a MiniKV node.
type Config struct {
	NodeID     string
	ListenAddr string
	DataDir    string
}

// Default returns a development-friendly single-node configuration.
func Default() Config {
	return Config{
		NodeID:     "node-1",
		ListenAddr: "127.0.0.1:8080",
		DataDir:    "data",
	}
}
