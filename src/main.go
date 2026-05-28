package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"portrelay/client"
	"portrelay/config"
	"portrelay/server"
	"portrelay/update"
)

// Set via -ldflags at build time.
var version = "dev"

func main() {
	configPath := flag.String("config", "", "Path to configuration file")
	configBase64 := flag.String("config-base64", "", "Base64-encoded configuration JSON")
	flag.Parse()

	var loadMode config.LoadMode
	var loadValue string

	if *configBase64 != "" {
		loadMode = config.LoadBase64
		loadValue = *configBase64
	} else if *configPath != "" {
		loadMode = config.LoadPath
		loadValue = *configPath
	} else {
		loadMode = config.LoadDefault
	}

	cfg, err := config.LoadConfig(loadMode, loadValue)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	log.Printf("PortRelay starting: name=\"%s\" mode=%s proxies=%d version=%s", cfg.Name, cfg.Mode, len(cfg.Proxies), version)

	if cfg.CheckUpdate {
		update.CheckForUpdate(version)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	switch cfg.Mode {
	case "server":
		runServer(cfg, sigCh)
	case "client":
		runClient(cfg, sigCh)
	default:
		log.Fatalf("Unknown mode: %s", cfg.Mode)
	}
}

func runServer(cfg *config.Config, sigCh chan os.Signal) {
	srv := server.New(cfg)

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Start()
	}()

	select {
	case <-sigCh:
		log.Println("Shutting down server...")
		srv.Shutdown()
	case err := <-errCh:
		if err != nil {
			log.Fatalf("Server error: %v", err)
		}
	}

	log.Println("Server stopped")
}

func runClient(cfg *config.Config, sigCh chan os.Signal) {
	var clients []interface{ Stop() }
	errCh := make(chan error, 1)

	for i := range cfg.Proxies {
		proxy := &cfg.Proxies[i]

		if proxy.Type == "direct" {
			if cfg.Mode != "client" {
				log.Printf("Warning: skipping direct proxy \"%s\" in non-client mode", proxy.Name)
				continue
			}
			dc := client.NewDirectClient(proxy)
			clients = append(clients, dc)
			go func() {
				if err := dc.Start(); err != nil {
					errCh <- fmt.Errorf("direct proxy \"%s\": %w", proxy.Name, err)
				}
			}()
		} else if proxy.Type == "tunnel" {
			if cfg.Mode == "client" {
				tc := client.NewTunnelClient(proxy)
				clients = append(clients, tc)
				go func() {
					if err := tc.Start(); err != nil {
						errCh <- fmt.Errorf("tunnel proxy \"%s\": %w", proxy.Name, err)
					}
				}()
			}
			// Server tunnel proxies are handled by the server
		}
	}

	select {
	case <-sigCh:
		log.Println("Shutting down client...")
		for _, c := range clients {
			c.Stop()
		}
	case err := <-errCh:
		if err != nil {
			log.Fatalf("Client error: %v", err)
		}
	}

	log.Println("Client stopped")
}
