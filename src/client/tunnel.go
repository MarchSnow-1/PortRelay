package client

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"

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
	sessionID   uint32
	remoteAddr  net.Addr
	createdAt   time.Time
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
		log.Printf("[Tunnel \"%s\"] Local UDP listener on %s", c.proxy.Name, c.proxy.ListenLocal)
	}

	if needTCP {
		l, err := net.Listen("tcp", c.proxy.ListenLocal)
		if err != nil {
			return fmt.Errorf("failed to listen TCP on %s: %w", c.proxy.ListenLocal, err)
		}
		c.tcpListener = l
		log.Printf("[Tunnel \"%s\"] Local TCP listener on %s", c.proxy.Name, c.proxy.ListenLocal)
	}

	log.Printf("[Tunnel \"%s\"] Listen protocol: %s | Transport: %s | Server: %s",
		c.proxy.Name, c.proxy.ListenProtocol, c.proxy.Transport, c.proxy.ServerIP)

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
			log.Printf("[Tunnel \"%s\"] Connection failed: %v, retrying in %v...", c.proxy.Name, err, c.reconnectInterval)
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
		log.Printf("[Tunnel \"%s\"] Connected | server=%s | transport=%s | listen=%s",
			c.proxy.Name, c.proxy.ServerIP, transportName, c.proxy.ListenProtocol)

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
		log.Printf("[Tunnel \"%s\"] Transport connection lost, reconnecting...", c.proxy.Name)

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
			log.Printf("[Tunnel \"%s\"] Warning: TCP transport failed, falling back to UDP", c.proxy.Name)
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
	log.Printf("[Tunnel \"%s\"] Establishing KCP connection (TCP-in-UDP mode)", c.proxy.Name)

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
				log.Printf("[Tunnel \"%s\"] Local UDP read error: %v", c.proxy.Name, err)
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
				log.Printf("[Tunnel \"%s\"] Local TCP accept error: %v", c.proxy.Name, err)
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
	log.Printf("[Tunnel \"%s\"] New session | id=0x%08X | mode=TCP-in-%s | from=%s",
		c.proxy.Name, sessionID, transportName, conn.RemoteAddr().String())

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
				log.Printf("[Tunnel \"%s\"] Failed to parse UDP frame: %v", c.proxy.Name, err)
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
			log.Printf("[Tunnel \"%s\"] Failed to decode data: %v", c.proxy.Name, err)
			return
		}
		c.deliverToLocal(d)

	case protocol.FrameClose:
		cf, err := protocol.DecodeClose(frame.Payload)
		if err != nil {
			return
		}
		log.Printf("[Tunnel \"%s\"] Close frame: session=%d reason=%d", c.proxy.Name, cf.SessionID, cf.Reason)
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
	log.Printf("[Tunnel \"%s\"] New session | id=0x%08X | mode=UDP-in-%s | from=%s",
		c.proxy.Name, sessionID, transportName, addr.String())

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
