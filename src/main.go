package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	logger "github.com/donnie4w/go-logger/logger"

	"github.com/MarchSnow-1/PortRelay/client"
	"github.com/MarchSnow-1/PortRelay/config"
	"github.com/MarchSnow-1/PortRelay/server"
	"github.com/MarchSnow-1/PortRelay/update"
)

// Set via -ldflags at build time.
var version = "dev"

func initLogger(level string) {
	logLevel := logger.LEVEL_INFO
	switch level {
	case "debug":
		logLevel = logger.LEVEL_DEBUG
	case "warn":
		logLevel = logger.LEVEL_WARN
	case "error":
		logLevel = logger.LEVEL_ERROR
	case "fatal":
		logLevel = logger.LEVEL_FATAL
	}

	levelFmt := func(level logger.LEVELTYPE) string {
		switch level {
		case logger.LEVEL_DEBUG:
			return "[DEBUG]"
		case logger.LEVEL_INFO:
			return "[INFO] "
		case logger.LEVEL_WARN:
			return "[WARN] "
		case logger.LEVEL_ERROR:
			return "[ERROR]"
		case logger.LEVEL_FATAL:
			return "[FATAL]"
		default:
			return "[?????]"
		}
	}

	format := logger.FORMAT_LEVELFLAG | logger.FORMAT_DATE | logger.FORMAT_TIME
	formatter := "{level} {time} {message}\n"
	if level == "debug" {
		format |= logger.FORMAT_SHORTFILENAME
		formatter = "{level} {time} {file} {message}\n"
	}

	logger.SetOption(&logger.Option{
		Level:      logLevel,
		Console:    true,
		Format:     format,
		Formatter:  formatter,
		AttrFormat: &logger.AttrFormat{SetLevelFmt: levelFmt},
	})
}

func main() {
	configPath := flag.String("config-path", "", "Path to configuration file")
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
		fmt.Printf("[FATAL] Failed to load configuration: %v\n", err)
		os.Exit(1)
	}

	logLevel := cfg.LogLevel
	if logLevel == "" {
		logLevel = "info"
	}
	initLogger(logLevel)

	logger.Info("PortRelay starting: name=\"", cfg.Name, "\" mode=", cfg.Mode, " proxies=", len(cfg.Proxies), " version=", version)

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
		logger.Fatal("Unknown mode: ", cfg.Mode)
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
		logger.Info("Shutting down server...")
		srv.Shutdown()
	case err := <-errCh:
		if err != nil {
			logger.Fatal("Server error: ", err)
		}
	}

	logger.Info("Server stopped")
}

func runClient(cfg *config.Config, sigCh chan os.Signal) {
	var clients []interface{ Stop() }
	errCh := make(chan error, 1)

	for i := range cfg.Proxies {
		proxy := &cfg.Proxies[i]

		if proxy.Type == "direct" {
			if cfg.Mode != "client" {
				logger.Warn("Skipping direct proxy \"", proxy.Name, "\" in non-client mode")
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
		}
	}

	select {
	case <-sigCh:
		logger.Info("Shutting down client...")
		for _, c := range clients {
			c.Stop()
		}
	case err := <-errCh:
		if err != nil {
			logger.Fatal("Client error: ", err)
		}
	}

	logger.Info("Client stopped")
}
