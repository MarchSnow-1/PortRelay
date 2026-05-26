package config

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type Config struct {
	Name           string  `json:"name"`
	Mode           string  `json:"mode"`
	Proxies        []Proxy `json:"proxies"`
	AdminPasswd    string  `json:"admin_passwd,omitempty"`
	ListenPort     string  `json:"listen_port,omitempty"`
	ListenProtocol string  `json:"listen_protocol,omitempty"`
}

type Proxy struct {
	Name string `json:"name"`
	Type string `json:"type"`
	// Server tunnel fields
	ServiceTarget string `json:"service_target,omitempty"`
	AllowProtocol string `json:"allow_protocol,omitempty"`
	Passwd        string `json:"passwd,omitempty"`
	// Client tunnel fields
	ListenProtocol string `json:"listen_protocol,omitempty"`
	ListenLocal    string `json:"listen_local,omitempty"`
	ServerIP       string `json:"server_ip,omitempty"`
	ServerPasswd   string `json:"server_passwd,omitempty"`
	Transport      string `json:"transport,omitempty"`
	// Client direct fields
	Protocol string `json:"protocol,omitempty"`
	Listen   string `json:"listen,omitempty"`
	Target   string `json:"target,omitempty"`
}

type LoadMode int

const (
	LoadDefault LoadMode = iota
	LoadPath
	LoadBase64
)

var (
	ErrInvalidMode        = errors.New("mode must be 'client' or 'server'")
	ErrEmptyProxies       = errors.New("proxies must not be empty")
	ErrInvalidProxyType   = errors.New("proxy type must be 'tunnel' or 'direct'")
	ErrMissingListenPort  = errors.New("server mode requires listen_port")
	ErrInvalidListenProto = errors.New("listen_protocol must be 'tcp', 'udp', or 'all'")
	ErrMissingTunnelName  = errors.New("tunnel proxy requires a name")
	ErrEmptyTunnelPasswd  = errors.New("tunnel proxy passwd must not be empty")
	ErrEmptyServerPasswd  = errors.New("client tunnel proxy server_passwd must not be empty")
)

func LoadConfig(loadMode LoadMode, value string) (*Config, error) {
	var data []byte
	var err error

	switch loadMode {
	case LoadDefault:
		execPath, err := os.Executable()
		if err != nil {
			return nil, fmt.Errorf("failed to get executable path: %w", err)
		}
		configPath := filepath.Join(filepath.Dir(execPath), "config.json")
		data, err = os.ReadFile(configPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read default config %s: %w", configPath, err)
		}
	case LoadPath:
		data, err = os.ReadFile(value)
		if err != nil {
			return nil, fmt.Errorf("failed to read config file %s: %w", value, err)
		}
	case LoadBase64:
		data, err = base64.StdEncoding.DecodeString(value)
		if err != nil {
			return nil, fmt.Errorf("failed to decode base64 config: %w", err)
		}
	default:
		return nil, fmt.Errorf("unknown config load mode: %d", loadMode)
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func (c *Config) Validate() error {
	if c.Mode != "client" && c.Mode != "server" {
		return ErrInvalidMode
	}
	if len(c.Proxies) == 0 {
		return ErrEmptyProxies
	}

	for i := range c.Proxies {
		if err := c.Proxies[i].Validate(c.Mode); err != nil {
			return fmt.Errorf("proxy[%d] (%s): %w", i, c.Proxies[i].Name, err)
		}
	}

	if c.Mode == "server" {
		if c.ListenPort == "" {
			return ErrMissingListenPort
		}
		if c.ListenProtocol != "tcp" && c.ListenProtocol != "udp" && c.ListenProtocol != "all" {
			return ErrInvalidListenProto
		}
	}

	return nil
}

func (p *Proxy) Validate(mode string) error {
	if p.Type != "tunnel" && p.Type != "direct" {
		return ErrInvalidProxyType
	}

	if p.Type == "tunnel" {
		if p.Name == "" {
			return ErrMissingTunnelName
		}
		if mode == "server" {
			if p.ServiceTarget == "" {
				return errors.New("server tunnel requires service_target")
			}
			if p.AllowProtocol != "tcp" && p.AllowProtocol != "udp" && p.AllowProtocol != "all" {
				return errors.New("server tunnel allow_protocol must be 'tcp', 'udp', or 'all'")
			}
			if p.Passwd == "" {
				return ErrEmptyTunnelPasswd
			}
		}
		if mode == "client" {
			if p.ListenProtocol != "tcp" && p.ListenProtocol != "udp" && p.ListenProtocol != "all" {
				return errors.New("client tunnel listen_protocol must be 'tcp', 'udp', or 'all'")
			}
			if p.ListenLocal == "" {
				return errors.New("client tunnel requires listen_local")
			}
			if p.ServerIP == "" {
				return errors.New("client tunnel requires server_ip")
			}
			if p.ServerPasswd == "" {
				return ErrEmptyServerPasswd
			}
			if p.Transport != "tcp" && p.Transport != "udp" && p.Transport != "auto" {
				return errors.New("client tunnel transport must be 'tcp', 'udp', or 'auto'")
			}
		}
	}

	if p.Type == "direct" {
		if mode == "server" {
			return errors.New("direct proxy is only valid in client mode")
		}
		if p.Name == "" {
			return errors.New("direct proxy requires a name")
		}
		if p.Protocol != "tcp" && p.Protocol != "udp" && p.Protocol != "all" {
			return errors.New("direct proxy protocol must be 'tcp', 'udp', or 'all'")
		}
		if p.Listen == "" {
			return errors.New("direct proxy requires listen address")
		}
		if p.Target == "" {
			return errors.New("direct proxy requires target address")
		}
	}

	return nil
}
