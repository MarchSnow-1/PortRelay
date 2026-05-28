<div align="center">

# PortRelay

基于 Golang 开发, 一款跨协议的轻量级端口转发与隧道工具

<!-- Badges -->

[![Platform](https://img.shields.io/badge/Platform-Windows%20%7C%20macOS%20%7C%20Linux-blue?style=for-the-badge)](https://github.com/MarchSnow-1/PortRelay)
[![Golang](https://img.shields.io/badge/Golang-1.24%2B-green?style=for-the-badge)](https://go.dev)
[![License](https://img.shields.io/badge/License-Apache%202.0-orange?style=for-the-badge)](LICENSE)
<br>
[![GitHub Release](https://img.shields.io/github/v/release/MarchSnow-1/PortRelay?style=for-the-badge)](https://github.com/MarchSnow-1/PortRelay/releases)
[![GitHub Repo stars](https://img.shields.io/github/stars/MarchSnow-1/PortRelay?style=for-the-badge)](https://github.com/MarchSnow-1/PortRelay)
[![GitHub Last Commit](https://img.shields.io/github/last-commit/MarchSnow-1/PortRelay?style=for-the-badge)](https://github.com/MarchSnow-1/PortRelay)

[**English**](README.md) | [**简体中文**](README_zh-CN.md)

</div>

## 项目简介

PortRelay 是一个 Go 语言编写的轻量级网络工具, 支持: 

- **UDP-over-TCP 隧道** — 将 UDP 流量封装在 TCP 连接中传输, 适用于仅开放 TCP 的受限网络（如部分云服务器、企业内网）
- **TCP-over-UDP 隧道** — 将 TCP 流量封装在 UDP 中传输, 借助 KCP 协议降低延迟, 适用于对延迟敏感的场景（如游戏、实时通信）
- **IPv4 ↔ IPv6 桥接** — 在两个地址族之间做固定端口转发, 适用于纯 IPv6 机器访问 IPv4 服务, 或 IPv6-only 环境部署需要 IPV4 传输数据的程序等场景
- **灵活部署** — 客户端与服务端分离部署, 客户端也可脱离服务端独立运行

## 快速开始（使用 Release 二进制）

从 [Releases](https://github.com/MarchSnow-1/PortRelay/releases) 下载对应平台的二进制, 直接运行: 

```bash
# 服务端
./portrelay --config server.json

# 客户端
./portrelay --config client.json

# 内联配置（无需文件）
./portrelay --config-base64 <base64编码的JSON>
```

## 配置文件

PortRelay 支持三种配置传入方式（优先级从高到低）: 

| 优先级 | CLI 参数 | 适用场景 |
|--------|----------|----------|
| 1 | `--config-base64 <base64>` | 脚本调用、容器部署、无文件环境 |
| 2 | `--config <path>` | 指定配置文件路径 |
| 3 | （无参数） | 自动读取程序同目录下的 `config.json` |

### 服务端

> [!WARNING]
> 如需复制配置文件，请删除注释

```json
{
  "name": "游戏服务中继", // 配置名称, 仅用于日志
  "mode": "server", // 运行模式: server
  "admin_passwd": "", // 全局通用密码, 空字符串 = 禁用
  "check_update": true, // 启动时检查新版本, 不填或 false 则不检查
  "listen_port": "9000", // 统一入口端口, TCP 和 UDP 共用
  "listen_protocol": "all", // 监听的传输协议: "tcp" / "udp" / "all"
  "proxies": [
    {
      "name": "cs2-tunnel", // 隧道名称, 客户端需匹配
      "type": "tunnel", // 固定值
      "service_target": "127.0.0.1:23450", // 最终转发目标 ip:port
      "allow_protocol": "udp", // 允许的内层协议: "tcp" / "udp" / "all"
      "passwd": "my-secret-key" // 隧道专属密码, 不可为空
    }
  ]
}
```

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `name` | string | 是 | 配置名称, 仅用于日志 |
| `mode` | string | 是 | `"server"` |
| `admin_passwd` | string | 否 | 全局通用密码, 空字符串 = 禁用 |
| `check_update` | bool | 否 | 启动时检查是否有新版本可用, 默认 `false` |
| `listen_port` | string | 是 | 统一入口端口, 同时监听 TCP 和 UDP |
| `listen_protocol` | string | 是 | 服务端监听的传输协议: `"tcp"` / `"udp"` / `"all"` |
| `proxies[].name` | string | 是 | 隧道名称 (客户端需匹配) |
| `proxies[].type` | string | 是 | `"tunnel"` |
| `proxies[].service_target` | string | 是 | 转发到的目标 `ip:port` |
| `proxies[].allow_protocol` | string | 是 | 内层协议: `"tcp"` / `"udp"` / `"all"` |
| `proxies[].passwd` | string | 是 | 隧道专属密码（不可为空） |

**认证顺序**: 先比对隧道密码 → 再比对全局 `admin_passwd`

**行为说明**:

- `listen_port` 是服务端统一入口, TCP 和 UDP 共用此端口
- `listen_protocol` 决定物理层监听哪些传输协议, 是第一层过滤: 客户端使用服务端未监听的传输协议时, 无法成功连接
- `allow_protocol` 是第二层过滤: 认证通过后, 数据帧的内层协议（TCP/UDP）必须匹配此字段, 不匹配则丢弃流量
- 客户端发来的数据帧 → 解封内层载荷 → 转发至 `service_target` → 收到的响应封装后沿隧道回传
- 同一隧道可被多个客户端同时连接, 通过 SessionID 区分不同数据流

### 客户端 — 隧道模式

> [!WARNING]
> 如需复制配置文件，请删除注释

```json
{
  "name": "我的客户端", // 配置名称, 仅用于日志
  "mode": "client", // 运行模式: client
  "check_update": true, // 启动时检查新版本, 不填或 false 则不检查
  "proxies": [
    {
      "name": "cs2-tunnel", // 隧道名称, 必须与服务端一致
      "type": "tunnel", // 固定值
      "listen_protocol": "udp", // 本地监听协议: "tcp" / "udp" / "all"
      "listen_local": "0.0.0.0:12345", // 本地监听地址, 发往此端口的数据将进入隧道
      "server_ip": "[2001:db8::1]:9000", // 服务端地址 [ip]:port
      "server_passwd": "my-secret-key", // 认证密码
      "transport": "auto" // 传输协议: "tcp" / "udp" / "auto"
    }
  ]
}
```

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `check_update` | bool | 否 | 启动时检查是否有新版本可用, 默认 `false` |
| `name` | string | 是 | 必须与服务端隧道名称一致 |
| `type` | string | 是 | `"tunnel"` |
| `listen_protocol` | string | 是 | 本地监听协议: `"tcp"` / `"udp"` / `"all"` |
| `listen_local` | string | 是 | 本地监听地址 `ip:port` |
| `server_ip` | string | 是 | 服务端地址 `[ipv6]:port` 或 `ip:port` |
| `server_passwd` | string | 是 | 发往服务端的认证密码 |
| `transport` | string | 是 | 隧道传输协议: `"tcp"` / `"udp"` / `"auto"` |

**`transport` 协商行为**: 

| 值 | 行为 |
|----|------|
| `"tcp"` | 强制 TCP, 服务端不支持时自动 fallback（打印 warning） |
| `"udp"` | 强制 UDP, 同上 fallback 逻辑 |
| `"auto"` | 优先匹配 `listen_protocol` 相同的协议；否则选另一种 |

**连接始终建立, 不会因协议不匹配而退出**

**行为说明**:

- 客户端在 `listen_local` 上按 `listen_protocol` 启动本地监听: `"tcp"` 仅 TCP, `"udp"` 仅 UDP, `"all"` 同时监听 TCP+UDP
- 启动时立即连接服务端并发送握手帧（隧道名 + 密码 + 期望传输协议）, 认证通过后开始数据中继
- `transport` 决定客户端与服务端之间的传输协议: `"tcp"` 走 TCP 流, `"udp"` 走 UDP 数据报, `"auto"` 自动协商
- 每条独立的本地数据流（不同源 IP/端口）分配一个 SessionID, 封装进数据帧发往服务端; 回传数据按 SessionID 路由回正确的来源
- 当 `transport` 为 `"tcp"` 且连接断开时, 客户端无限次自动重连, 无需人工干预
- `transport: "tcp"` 且服务端仅支持 UDP 时, 自动通过 KCP 建立 TCP-in-UDP 隧道

### 客户端 — 独立模式

> [!WARNING]
> 如需复制配置文件，请删除注释

无需服务端, 用于固定目标的 IPv4 ↔ IPv6 端口转发

```json
{
  "name": "单转发模式客户端",
  "mode": "client",
  "check_update": true, // 启动时检查新版本, 不填或 false 则不检查
  "proxies": [
    {
      "name": "v4-to-v6-tcp", // 你的配置文件命名
      "type": "direct", // 直接转发模式
      "protocol": "tcp", // 要转发的端口的协议
      "listen": "0.0.0.0:8080", // 本地监听端口, 发往此端口的数据将全部转发至 Target
      "target": "[2001:db8::2]:80" // 远端目标地址
    }
  ]
}
```

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `check_update` | bool | 否 | 启动时检查是否有新版本可用, 默认 `false` |
| `name` | string | 是 | 规则名称 |
| `type` | string | 是 | `"direct"` |
| `protocol` | string | 是 | `"tcp"` / `"udp"` / `"all"`（同时转发 TCP+UDP）|
| `listen` | string | 是 | 本地监听地址 `ip:port` |
| `target` | string | 是 | 远端目标 `[ipv6]:port` 或 `ip:port` |

**行为说明**:

- 在 `listen` 地址上启动监听, 所有流量直接转发至 `target`, 回传数据原路返回
- `protocol: "tcp"` — 每个新 TCP 连接独立处理, 双向透明转发
- `protocol: "udp"` — 按来源地址区分不同客户端, 各自维持独立会话
- `protocol: "all"` — 同一端口同时转发 TCP+UDP, 两端互不干扰
- 此模式无认证、无协议封装, 不依赖服务端, 客户端可单独运行

### 混合模式

> [!WARNING]
> 如需复制配置文件，请删除注释

同一客户端可同时包含隧道和独立模式规则, 各规则独立运行互不干扰: 

```json
{
  "name": "多模式客户端",
  "mode": "client",
  "check_update": true, // 启动时检查新版本, 不填或 false 则不检查
  "proxies": [
    {
      "name": "cs2-tunnel", // 隧道规则, 连接服务端
      "type": "tunnel",
      "listen_protocol": "udp",
      "listen_local": "0.0.0.0:12345",
      "server_ip": "[2001:db8::1]:9000",
      "server_passwd": "my-secret-key",
      "transport": "auto"
    },
    {
      "name": "dns-forward", // 独立规则, 无需服务端
      "type": "direct",
      "protocol": "udp",
      "listen": "0.0.0.0:5353",
      "target": "[2001:db8::2]:53"
    }
  ]
}
```

**行为说明**:

- 每条规则独立启动各自的监听和服务, 互不影响
- 隧道规则按隧道模式运行（需服务端）, 独立规则按直接转发模式运行（无需服务端）

### 地址格式

| 场景 | 格式 | 示例 |
|------|------|------|
| IPv4 | `ip:port` | `127.0.0.1:9000` |
| IPv6 | `[ipv6]:port` | `[2001:db8::1]:9000` |
| 监听所有接口 (IPv4) | `0.0.0.0:port` | `0.0.0.0:12345` |
| 监听所有接口 (IPv6) | `[::]:port` | `[::]:9000` |

## 传输模式

四种传输组合, 对应不同的可靠性保证: 

| 模式 | 内层 | 外层 | 可靠性 | 实现方式 |
|------|------|------|--------|----------|
| UDP in UDP | UDP | UDP | 尽力转发 | 原生 UDP socket |
| UDP in TCP | UDP | TCP | 尽力转发（模拟无连接） | TCP 流 + 断线自动重连 |
| TCP in TCP | TCP | TCP | 原生 TCP 保证 | TCP socket |
| TCP in UDP | TCP | UDP | Stop-and-Wait ARQ | [kcp-go](https://github.com/xtaci/kcp-go) |

**UDP in TCP 断线重连**: TCP 隧道断开后客户端自动重连, 无限次重试, 服务端将每次重连视为新连接

## 检查更新

在配置文件中设置 `"check_update": true` 后, 每次启动时程序会通过 GitHub API 获取最新 Release 版本号并与当前版本对比, 若有新版本将在终端打印提示:

```
2026/05/28 20:14:22 [Update] New version available: 1.0.0 (current: 0.0.6)
2026/05/28 20:14:22 [Update] Download: https://github.com/MarchSnow-1/PortRelay/releases
```

默认不启用, 不填写或设为 `false` 时启动不会发起任何网络请求

## 从源码构建

### 环境要求

| 依赖 | 说明 |
|------|------|
| Go | ≥ 1.24 |

### 构建

```bash
git clone https://github.com/MarchSnow-1/PortRelay.git
cd PortRelay/src
go build -o ../portrelay .
```

## 开源协议

Apache 2.0 — 详见 [LICENSE](LICENSE)。
