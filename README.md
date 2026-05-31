<div align="center">

# PortRelay

A lightweight cross-protocol port forwarding and tunneling tool built with Go.

<!-- Badges -->

[![Platform](https://img.shields.io/badge/Platform-Windows%20%7C%20macOS%20%7C%20Linux-blue?style=for-the-badge)](https://github.com/MarchSnow-1/PortRelay)
[![Golang](https://img.shields.io/badge/Golang-1.24%2B-green?style=for-the-badge)](https://go.dev)
[![License](https://img.shields.io/badge/License-Apache%202.0-orange?style=for-the-badge)](LICENSE)
<br>
[![GitHub Release](https://img.shields.io/github/v/release/MarchSnow-1/PortRelay?style=for-the-badge)](https://github.com/MarchSnow-1/PortRelay/releases)
[![GitHub Repo stars](https://img.shields.io/github/stars/MarchSnow-1/PortRelay?style=for-the-badge)](https://github.com/MarchSnow-1/PortRelay)
[![GitHub Last Commit](https://img.shields.io/github/last-commit/MarchSnow-1/PortRelay?style=for-the-badge)](https://github.com/MarchSnow-1/PortRelay)
[![Total Download](https://img.shields.io/github/downloads/MarchSnow-1/PortRelay/total?style=for-the-badge)](https://github.com/MarchSnow-1/PortRelay/releases)

[**English**](README.md) | [**简体中文**](README_zh-CN.md)

</div>

## Overview

PortRelay is a lightweight network tool written in Go, supporting:

- **UDP-over-TCP tunneling** — Encapsulate UDP traffic inside TCP connections, ideal for restricted networks that only allow TCP (e.g. cloud servers, corporate intranets)
- **TCP-over-UDP tunneling** — Encapsulate TCP traffic inside UDP with KCP protocol for lower latency, suitable for latency-sensitive scenarios (e.g. gaming, real-time communication)
- **IPv4 ↔ IPv6 bridging** — Fixed port forwarding between address families, enabling IPv6-only machines to reach IPv4 services, or deploying IPv4-dependent applications in IPv6-only environments
- **Flexible deployment** — Client and server can run separately, or the client can run standalone

## Quick Start (Release Binary)

Download the binary for your platform from [Releases](https://github.com/MarchSnow-1/PortRelay/releases) and run:

```bash
# Server
./portrelay --config-path server.json

# Client
./portrelay --config-path client.json

# Inline config (no files needed)
./portrelay --config-base64 <base64-encoded-json>
```

## Configuration

PortRelay supports three config loading modes (priority high to low):

| Priority | CLI flag | Use case |
|----------|----------|----------|
| 1 | `--config-base64 <base64>` | Scripts, containers, no filesystem |
| 2 | `--config-path <path>` | Custom config path |
| 3 | *(none)* | Reads `config.json` alongside the binary |

### Server

> [!WARNING]
> Remove comments before using this config file.

```json
{
  "name": "Game Server Relay", // Config label, logging only
  "mode": "server", // Run mode: server
  "admin_passwd": "", // Global fallback password, empty = disabled
  "check_update": true, // Check for updates on startup, omit or false to disable
  "log_level": "info", // Log level: "debug" / "info" / "warn" / "error" / "fatal"
  "listen_port": "9000", // Single entry port for both TCP and UDP
  "listen_protocol": "all", // Transport to listen on: "tcp" / "udp" / "all"
  "proxies": [
    {
      "name": "cs2-tunnel", // Tunnel name, client must match
      "type": "tunnel", // Fixed value
      "service_target": "127.0.0.1:23450", // Final forward target ip:port
      "allow_protocol": "udp", // Allowed inner protocol: "tcp" / "udp" / "all"
      "passwd": "my-secret-key" // Per-tunnel password, required
    }
  ]
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | yes | Config label, logging only |
| `mode` | string | yes | `"server"` |
| `admin_passwd` | string | no | Global fallback password. Empty = disabled |
| `check_update` | bool | no | Check for new version on startup. Default `false` |
| `log_level` | string | no | Log level: `"debug"` / `"info"` / `"warn"` / `"error"` / `"fatal"`. Default `"info"`. Shows filename and line number when set to `"debug"` |
| `listen_port` | string | yes | Single port for both TCP and UDP |
| `listen_protocol` | string | yes | Transport protocol to listen on: `"tcp"` / `"udp"` / `"all"` |
| `proxies[].name` | string | yes | Tunnel name (client must match) |
| `proxies[].type` | string | yes | `"tunnel"` |
| `proxies[].service_target` | string | yes | Forward target `ip:port` |
| `proxies[].allow_protocol` | string | yes | Inner protocol: `"tcp"` / `"udp"` / `"all"` |
| `proxies[].passwd` | string | yes | Per-tunnel password (required) |

**Authentication order**: tunnel password → global `admin_passwd` (if set). First match wins.

**How it works**:

- `listen_port` is the single entry point; both TCP and UDP share this port
- `listen_protocol` is the first filter — the server only listens on the specified transport protocol(s); clients using an unsupported transport cannot connect
- `allow_protocol` is the second filter — after authentication, each data frame's inner protocol (TCP/UDP) must match this field, or the traffic is dropped
- Incoming data frame → unwrap inner payload → forward to `service_target` → wrap response and send back through the tunnel
- Multiple clients can connect to the same tunnel simultaneously; SessionID multiplexes different data streams

### Client — Tunnel Mode

> [!WARNING]
> Remove comments before using this config file.

```json
{
  "name": "My Client", // Config label, logging only
  "mode": "client", // Run mode: client
  "check_update": true, // Check for updates on startup, omit or false to disable
  "log_level": "info", // Log level: "debug" / "info" / "warn" / "error" / "fatal"
  "proxies": [
    {
      "name": "cs2-tunnel", // Tunnel name, must match server
      "type": "tunnel", // Fixed value
      "listen_protocol": "udp", // Local listen protocol: "tcp" / "udp" / "all"
      "listen_local": "0.0.0.0:12345", // Local listen address, traffic here enters the tunnel
      "server_ip": "[2001:db8::1]:9000", // Server address [ip]:port
      "server_passwd": "my-secret-key", // Authentication password
      "transport": "auto" // Transport protocol: "tcp" / "udp" / "auto"
    }
  ]
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `check_update` | bool | no | Check for new version on startup. Default `false` |
| `log_level` | string | no | Log level: `"debug"` / `"info"` / `"warn"` / `"error"` / `"fatal"`. Default `"info"`. Shows filename and line number when set to `"debug"` |
| `name` | string | yes | Must match server tunnel name |
| `type` | string | yes | `"tunnel"` |
| `listen_protocol` | string | yes | Local listen protocol: `"tcp"` / `"udp"` / `"all"` |
| `listen_local` | string | yes | Local listen address `ip:port` |
| `server_ip` | string | yes | Server address `[ipv6]:port` or `ip:port` |
| `server_passwd` | string | yes | Password sent to server |
| `transport` | string | yes | Tunnel transport: `"tcp"` / `"udp"` / `"auto"` |

**`transport` negotiation**:

| Value | Behavior |
|-------|----------|
| `"tcp"` | Force TCP. Falls back if server doesn't support it (prints warning) |
| `"udp"` | Force UDP. Same fallback logic |
| `"auto"` | Prefer the protocol matching `listen_protocol`; otherwise pick the other |

**The connection always succeeds — no protocol mismatch will cause a hard exit.**

**How it works**:

- Starts local listener(s) on `listen_local` per `listen_protocol`: `"tcp"` TCP only, `"udp"` UDP only, `"all"` both TCP+UDP
- On startup, immediately connects to the server and sends a handshake frame (tunnel name + password + desired transport); data relay begins after authentication
- `transport` sets the protocol between client and server: `"tcp"` uses TCP streams, `"udp"` uses UDP datagrams, `"auto"` negotiates automatically
- Each distinct local data flow (unique source IP/port) gets a SessionID, wrapped into data frames sent to the server; responses are routed back by SessionID
- When `transport` is `"tcp"` and the connection drops, the client auto-reconnects indefinitely
- `transport: "tcp"` with a UDP-only server automatically establishes a TCP-in-UDP tunnel via KCP

### Client — Direct Mode

> [!WARNING]
> Remove comments before using this config file.

No server required. Fixed IPv4 ↔ IPv6 port forwarding.

```json
{
  "name": "Direct Forward Client",
  "mode": "client",
  "check_update": true, // Check for updates on startup, omit or false to disable
  "log_level": "info", // Log level: "debug" / "info" / "warn" / "error" / "fatal"
  "proxies": [
    {
      "name": "v4-to-v6-tcp", // Rule name
      "type": "direct", // Direct forwarding mode
      "protocol": "tcp", // Protocol to forward: "tcp" / "udp" / "all"
      "listen": "0.0.0.0:8080", // Local listen port, traffic here is forwarded to target
      "target": "[2001:db8::2]:80" // Remote target address
    }
  ]
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `check_update` | bool | no | Check for new version on startup. Default `false` |
| `log_level` | string | no | Log level: `"debug"` / `"info"` / `"warn"` / `"error"` / `"fatal"`. Default `"info"`. Shows filename and line number when set to `"debug"` |
| `name` | string | yes | Rule name |
| `type` | string | yes | `"direct"` |
| `protocol` | string | yes | `"tcp"` / `"udp"` / `"all"` (forwards both TCP+UDP) |
| `listen` | string | yes | Local listen address `ip:port` |
| `target` | string | yes | Remote target `[ipv6]:port` or `ip:port` |

**How it works**:

- Listens on `listen` address; all traffic is forwarded directly to `target`, responses return the same way
- `protocol: "tcp"` — each new TCP connection handled independently, bidirectional transparent forwarding
- `protocol: "udp"` — distinguishes clients by source address, maintaining separate sessions
- `protocol: "all"` — forwards both TCP+UDP on a single port, traffic does not interfere
- No authentication, no protocol encapsulation, no server dependency — the client runs standalone

### Mixed Mode

> [!WARNING]
> Remove comments before using this config file.

A single client config can combine tunnel and direct rules; each runs independently:

```json
{
  "name": "Multi-Mode Client",
  "mode": "client",
  "check_update": true, // Check for updates on startup, omit or false to disable
  "log_level": "info", // Log level: "debug" / "info" / "warn" / "error" / "fatal"
  "proxies": [
    {
      "name": "cs2-tunnel", // Tunnel rule, requires server
      "type": "tunnel",
      "listen_protocol": "udp",
      "listen_local": "0.0.0.0:12345",
      "server_ip": "[2001:db8::1]:9000",
      "server_passwd": "my-secret-key",
      "transport": "auto"
    },
    {
      "name": "dns-forward", // Direct rule, no server needed
      "type": "direct",
      "protocol": "udp",
      "listen": "0.0.0.0:5353",
      "target": "[2001:db8::2]:53"
    }
  ]
}
```

**How it works**:

- Each rule starts its own listeners and services independently
- Tunnel rules operate in tunnel mode (requires server), direct rules operate in forwarding mode (standalone)
- They do not interfere with each other

### Address Format

| Scenario | Format | Example |
|----------|--------|---------|
| IPv4 | `ip:port` | `127.0.0.1:9000` |
| IPv6 | `[ipv6]:port` | `[2001:db8::1]:9000` |
| All interfaces (IPv4) | `0.0.0.0:port` | `0.0.0.0:12345` |
| All interfaces (IPv6) | `[::]:port` | `[::]:9000` |

## Transport Modes

Four transport combinations with different reliability guarantees:

| Mode | Inner | Outer | Reliability | Implementation |
|------|-------|-------|-------------|----------------|
| UDP in UDP | UDP | UDP | Best-effort | Native UDP socket |
| UDP in TCP | UDP | TCP | Best-effort (connectionless) | TCP stream + auto-reconnect |
| TCP in TCP | TCP | TCP | Native TCP | TCP socket |
| TCP in UDP | TCP | UDP | Stop-and-Wait ARQ | [kcp-go](https://github.com/xtaci/kcp-go) |

**UDP-in-TCP auto-reconnect**: On TCP disconnect, the client reconnects indefinitely. The server treats each reconnect as a fresh connection.

## Update Check

Set `"check_update": true` in your config file. On each startup the program queries the GitHub API for the latest release tag and compares it with the current version. If a newer version is available, a reminder is printed:

```
[INFO]  2026/05/28 20:14:22 New version available: 1.0.0
[INFO]  2026/05/28 20:14:22 Download: https://github.com/MarchSnow-1/PortRelay/releases
```

Disabled by default — omit the field or set it to `false` and no network request is made at startup.

## Build from Source

### Requirements

| Dependency | Notes |
|------------|-------|
| Go | ≥ 1.24 |

### Build

```bash
git clone https://github.com/MarchSnow-1/PortRelay.git
cd PortRelay/src
go build -o ../portrelay .
```

## License

Apache 2.0 — see [LICENSE](LICENSE) for details.
