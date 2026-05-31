package client

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"

	logger "github.com/donnie4w/go-logger/logger"

	"github.com/MarchSnow-1/PortRelay/config"
	"github.com/MarchSnow-1/PortRelay/protocol"
	"github.com/MarchSnow-1/PortRelay/transport"
)

type TunnelClient struct {
	proxy  *config.Proxy
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Local UDP listener
	udpConn *net.UDPConn

	// Local TCP listener (for listen_protocol "all")
	tcpListener net.Listener

	// Transport connection to server
	transportConn net.Conn
	transportType byte // actual transport being used

	// Session tracking: localAddr -> sessionID
	sessions   map[string]*localSession
	sessionsMu sync.RWMutex

	// TCP session tracking: sessionID -> local TCP connection
	tcpConns   map[uint32]net.Conn
	tcpConnsMu sync.RWMutex

	// Reconnect control
	reconnectInterval time.Duration
	reconnecting      bool
	reconnectMu       sync.Mutex
}

type localSession struct {
	sessionID  uint32
	remoteAddr net.Addr
	createdAt  time.Time
}

func NewTunnelClient(proxy *config.Proxy) *TunnelClient {
	ctx, cancel := context.WithCancel(context.Background())
	return &TunnelClient{
		proxy:             proxy,
		ctx:               ctx,
		cancel:            cancel,
		sessions:          make(map[string]*localSession),
		tcpConns:          make(map[uint32]net.Conn),
		reconnectInterval: 3 * time.Second,
	}
}

func (c *TunnelClient) Start() error {
	needUDP := c.proxy.ListenProtocol == "udp" || c.proxy.ListenProtocol == "all"
	needTCP := c.proxy.ListenProtocol == "tcp" || c.proxy.ListenProtocol == "all"

	if needUDP {
		addr, err := net.ResolveUDPAddr("udp", c.proxy.ListenLocal)
		if err != nil {
			return fmt.Errorf("failed to resolve local UDP address %s: %w", c.proxy.ListenLocal, err)
		}
		conn, err := net.ListenUDP("udp", addr)
		if err != nil {
			return fmt.Errorf("failed to listen UDP on %s: %w", c.proxy.ListenLocal, err)
		}
		c.udpConn = conn
		logger.Info("[\"", c.proxy.Name, "\"] Local UDP listener on ", c.proxy.ListenLocal)
	}

	if needTCP {
		l, err := net.Listen("tcp", c.proxy.ListenLocal)
		if err != nil {
			return fmt.Errorf("failed to listen TCP on %s: %w", c.proxy.ListenLocal, err)
		}
		c.tcpListener = l
		logger.Info("[\"", c.proxy.Name, "\"] Local TCP listener on ", c.proxy.ListenLocal)
	}

	logger.Info("[\"", c.proxy.Name, "\"] Listen protocol: ", c.proxy.ListenProtocol,
		" | Transport: ", c.proxy.Transport, " | Server: ", c.proxy.ServerIP)

	return c.connectAndRelay()
}

func (c *TunnelClient) Stop() {
	c.cancel()
	if c.transportConn != nil {
		c.transportConn.Close()
	}
	if c.udpConn != nil {
		c.udpConn.Close()
	}
	if c.tcpListener != nil {
		c.tcpListener.Close()
	}
}

func (c *TunnelClient) connectAndRelay() error {
	for {
		select {
		case <-c.ctx.Done():
			return nil
		default:
		}

		if err := c.connect(); err != nil {
			logger.Error("[\"", c.proxy.Name, "\"] Connection failed: ", err, ", retrying in ", c.reconnectInterval, "...")
			select {
			case <-c.ctx.Done():
				return nil
			case <-time.After(c.reconnectInterval):
			}
			continue
		}

		c.reconnectMu.Lock()
		c.reconnecting = false
		c.reconnectMu.Unlock()

		transportName := "TCP"
		if c.transportType == protocol.TransportUDP {
			transportName = "UDP"
		}
		logger.Info("[\"", c.proxy.Name, "\"] Connected | server=", c.proxy.ServerIP,
			" | transport=", transportName, " | listen=", c.proxy.ListenProtocol)

		// Start local listeners
		if c.udpConn != nil {
			c.wg.Add(1)
			go c.handleLocalUDP()
		}
		if c.tcpListener != nil {
			c.wg.Add(1)
			go c.handleLocalTCP()
		}

		// Read from transport and forward to local
		c.readFromTransport()

		// Connection lost - clean up for reconnect
		logger.Warn("[\"", c.proxy.Name, "\"] Transport connection lost, reconnecting...")

		c.reconnectMu.Lock()
		c.reconnecting = true
		c.reconnectMu.Unlock()
	}
}

func (c *TunnelClient) connect() error {
	transportProto := c.proxy.Transport
	preferTCP := transportProto == "tcp"
	preferUDP := transportProto == "udp"

	// Try TCP first if preferred or auto
	if preferTCP || transportProto == "auto" {
		conn, err := net.DialTimeout("tcp", c.proxy.ServerIP, 10*time.Second)
		if err == nil {
			h := &protocol.Handshake{
				TunnelName:     c.proxy.Name,
				Passwd:         c.proxy.ServerPasswd,
				TransportProto: protocol.TransportTCP,
			}
			f := &protocol.Frame{Type: protocol.FrameHandshake, Payload: protocol.EncodeHandshake(h)}
			if err := protocol.WriteFrame(conn, f); err != nil {
				conn.Close()
				goto tryUDP
			}

			ackFrame, err := protocol.ReadFrame(conn)
			if err != nil {
				conn.Close()
				goto tryUDP
			}
			if ackFrame.Type != protocol.FrameHandshakeAck {
				conn.Close()
				goto tryUDP
			}
			ack, err := protocol.DecodeHandshakeAck(ackFrame.Payload)
			if err != nil {
				conn.Close()
				goto tryUDP
			}

			if ack.StatusCode == protocol.StatusOK {
				c.transportConn = conn
				c.transportType = protocol.TransportTCP
				return nil
			}

			conn.Close()
			if ack.StatusCode != protocol.StatusProtocolMismatch && ack.StatusCode != protocol.StatusOK {
				return fmt.Errorf("handshake rejected: status=%d", ack.StatusCode)
			}
		}
	}

tryUDP:
	if preferUDP || transportProto == "auto" || preferTCP {
		if preferTCP {
			logger.Warn("[\"", c.proxy.Name, "\"] TCP transport failed, falling back to UDP")
		}

		// Try UDP transport
		raddr, err := net.ResolveUDPAddr("udp", c.proxy.ServerIP)
		if err != nil {
			return fmt.Errorf("failed to resolve UDP address: %w", err)
		}

		conn, err := net.DialUDP("udp", nil, raddr)
		if err != nil {
			return fmt.Errorf("failed to dial UDP: %w", err)
		}

		h := &protocol.Handshake{
			TunnelName:     c.proxy.Name,
			Passwd:         c.proxy.ServerPasswd,
			TransportProto: protocol.TransportUDP,
		}
		f := &protocol.Frame{Type: protocol.FrameHandshake, Payload: protocol.EncodeHandshake(h)}
		if _, err := conn.Write(protocol.EncodeFrame(f)); err != nil {
			conn.Close()
			return fmt.Errorf("failed to send UDP handshake: %w", err)
		}

		conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		buf := make([]byte, 65535)
		n, err := conn.Read(buf)
		if err != nil {
			conn.Close()
			return fmt.Errorf("failed to read UDP handshake ack: %w", err)
		}
		conn.SetReadDeadline(time.Time{})

		ackFrame, err := protocol.ParseFrameFromDatagram(buf[:n])
		if err != nil {
			conn.Close()
			return fmt.Errorf("failed to parse UDP handshake ack: %w", err)
		}
		ack, err := protocol.DecodeHandshakeAck(ackFrame.Payload)
		if err != nil {
			conn.Close()
			return fmt.Errorf("failed to decode UDP handshake ack: %w", err)
		}

		if ack.StatusCode == protocol.StatusOK {
			if ack.AcceptedProto == protocol.TransportTCP {
				// Server wants KCP (TCP-in-UDP)
				conn.Close()
				return c.connectKCP()
			}

			c.transportConn = conn
			c.transportType = protocol.TransportUDP
			return nil
		}

		conn.Close()
		return fmt.Errorf("handshake rejected: status=%d", ack.StatusCode)
	}

	return fmt.Errorf("no transport available")
}

func (c *TunnelClient) connectKCP() error {
	logger.Info("[\"", c.proxy.Name, "\"] Establishing KCP connection (TCP-in-UDP mode)")

	kcpConn, err := transport.DialKCP(c.proxy.ServerIP)
	if err != nil {
		return fmt.Errorf("failed to dial KCP: %w", err)
	}

	c.transportConn = kcpConn
	c.transportType = protocol.TransportTCP
	return nil
}

func (c *TunnelClient) handleLocalUDP() {
	defer c.wg.Done()

	buf := make([]byte, 65535)
	for {
		n, remoteAddr, err := c.udpConn.ReadFrom(buf)
		if err != nil {
			select {
			case <-c.ctx.Done():
				return
			default:
			}
			c.reconnectMu.Lock()
			reconnecting := c.reconnecting
			c.reconnectMu.Unlock()
			if !reconnecting {
				logger.Error("[\"", c.proxy.Name, "\"] Local UDP read error: ", err)
			}
			continue
		}

		data := make([]byte, n)
		copy(data, buf[:n])

		sessionID := c.getOrCreateSession(remoteAddr)

		d := &protocol.Data{
			SessionID:    sessionID,
			InnerProto:   protocol.InnerProtoUDP,
			InnerPayload: data,
		}
		f := &protocol.Frame{Type: protocol.FrameData, Payload: protocol.EncodeData(d)}

		c.sendToTransport(f)
	}
}

func (c *TunnelClient) handleLocalTCP() {
	defer c.wg.Done()

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		conn, err := c.tcpListener.Accept()
		if err != nil {
			select {
			case <-c.ctx.Done():
				return
			default:
				logger.Error("[\"", c.proxy.Name, "\"] Local TCP accept error: ", err)
				continue
			}
		}

		sessionID := rand.Uint32()
		if sessionID == 0 {
			sessionID = 1
		}

		c.wg.Add(1)
		go c.handleLocalTCPConn(conn, sessionID)
	}
}

func (c *TunnelClient) handleLocalTCPConn(conn net.Conn, sessionID uint32) {
	defer c.wg.Done()
	defer conn.Close()
	defer func() {
		c.tcpConnsMu.Lock()
		delete(c.tcpConns, sessionID)
		c.tcpConnsMu.Unlock()

		cf := &protocol.CloseFrame{SessionID: sessionID, Reason: protocol.CloseNormal}
		f := &protocol.Frame{Type: protocol.FrameClose, Payload: protocol.EncodeClose(cf)}
		c.sendToTransport(f)
	}()

	c.tcpConnsMu.Lock()
	c.tcpConns[sessionID] = conn
	c.tcpConnsMu.Unlock()

	transportName := "TCP"
	if c.transportType == protocol.TransportUDP {
		transportName = "UDP"
	}
	logger.Info("[\"", c.proxy.Name, "\"] New session | id=0x", fmt.Sprintf("%08X", sessionID),
		" | mode=TCP-in-", transportName, " | from=", conn.RemoteAddr().String())

	buf := make([]byte, 65535)
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		n, err := conn.Read(buf)
		if err != nil {
			return
		}

		d := &protocol.Data{
			SessionID:    sessionID,
			InnerProto:   protocol.InnerProtoTCP,
			InnerPayload: buf[:n],
		}
		f := &protocol.Frame{Type: protocol.FrameData, Payload: protocol.EncodeData(d)}
		c.sendToTransport(f)
	}
}

func (c *TunnelClient) readFromTransport() {
	// TCP/KCP stream: read frames
	if c.transportType == protocol.TransportTCP {
		for {
			frame, err := protocol.ReadFrame(c.transportConn)
			if err != nil {
				return
			}
			c.processIncomingFrame(frame)
		}
	} else {
		// UDP transport: read datagrams
		conn := c.transportConn.(*net.UDPConn)
		buf := make([]byte, 65535)
		for {
			n, err := conn.Read(buf)
			if err != nil {
				return
			}
			frame, err := protocol.ParseFrameFromDatagram(buf[:n])
			if err != nil {
				logger.Error("[\"", c.proxy.Name, "\"] Failed to parse UDP frame: ", err)
				continue
			}
			c.processIncomingFrame(frame)
		}
	}
}

func (c *TunnelClient) processIncomingFrame(frame *protocol.Frame) {
	switch frame.Type {
	case protocol.FrameData:
		d, err := protocol.DecodeData(frame.Payload)
		if err != nil {
			logger.Error("[\"", c.proxy.Name, "\"] Failed to decode data: ", err)
			return
		}
		c.deliverToLocal(d)

	case protocol.FrameClose:
		cf, err := protocol.DecodeClose(frame.Payload)
		if err != nil {
			return
		}
		logger.Info("[\"", c.proxy.Name, "\"] Close frame: session=", cf.SessionID, " reason=", cf.Reason)
		c.removeSession(cf.SessionID)
	}
}

func (c *TunnelClient) deliverToLocal(d *protocol.Data) {
	// Check TCP sessions first
	c.tcpConnsMu.RLock()
	conn, ok := c.tcpConns[d.SessionID]
	c.tcpConnsMu.RUnlock()
	if ok {
		conn.Write(d.InnerPayload)
		return
	}

	c.sessionsMu.RLock()
	defer c.sessionsMu.RUnlock()

	// Find session by ID
	for _, sess := range c.sessions {
		if sess.sessionID == d.SessionID {
			if c.udpConn != nil {
				c.udpConn.WriteTo(d.InnerPayload, sess.remoteAddr)
			}
			return
		}
	}
}

func (c *TunnelClient) sendToTransport(f *protocol.Frame) {
	if c.transportConn == nil {
		return
	}

	c.reconnectMu.Lock()
	reconnecting := c.reconnecting
	c.reconnectMu.Unlock()
	if reconnecting {
		return
	}

	if c.transportType == protocol.TransportTCP {
		protocol.WriteFrame(c.transportConn, f)
	} else {
		c.transportConn.Write(protocol.EncodeFrame(f))
	}
}

func (c *TunnelClient) getOrCreateSession(addr net.Addr) uint32 {
	key := addr.String()

	c.sessionsMu.RLock()
	sess, ok := c.sessions[key]
	c.sessionsMu.RUnlock()
	if ok {
		return sess.sessionID
	}

	c.sessionsMu.Lock()
	defer c.sessionsMu.Unlock()

	if sess, ok := c.sessions[key]; ok {
		return sess.sessionID
	}

	sessionID := rand.Uint32()
	if sessionID == 0 {
		sessionID = 1
	}

	c.sessions[key] = &localSession{
		sessionID:  sessionID,
		remoteAddr: addr,
		createdAt:  time.Now(),
	}

	transportName := "TCP"
	if c.transportType == protocol.TransportUDP {
		transportName = "UDP"
	}
	logger.Info("[\"", c.proxy.Name, "\"] New session | id=0x", fmt.Sprintf("%08X", sessionID),
		" | mode=UDP-in-", transportName, " | from=", addr.String())

	return sessionID
}

func (c *TunnelClient) removeSession(sessionID uint32) {
	c.sessionsMu.Lock()
	defer c.sessionsMu.Unlock()

	for key, sess := range c.sessions {
		if sess.sessionID == sessionID {
			delete(c.sessions, key)
			return
		}
	}
}
