package server

import (
	"context"
	"crypto/subtle"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	logger "github.com/donnie4w/go-logger/logger"

	"github.com/MarchSnow-1/PortRelay/config"
	"github.com/MarchSnow-1/PortRelay/protocol"
	"github.com/MarchSnow-1/PortRelay/transport"
)

type Server struct {
	cfg     *config.Config
	tunnels map[string]*config.Proxy

	tcpListener net.Listener
	udpConn     net.PacketConn

	serviceConns   map[string]net.Conn
	serviceConnsMu sync.Mutex

	udpClients   map[string]*udpClient
	udpClientsMu sync.RWMutex

	kcpSessions   map[uint32]*kcpServerSession
	kcpSessionsMu sync.Mutex

	pendingKCP   map[string]*pendingHandshake
	pendingKCPMu sync.Mutex

	sessions   map[uint32]*sessionState
	sessionsMu sync.RWMutex

	activeConnections int64 // atomic counter
	maxConnections    int64 // from config

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type udpClient struct {
	tunnel     *config.Proxy
	addr       net.Addr
	createdAt  time.Time
	lastActive time.Time
}

type pendingHandshake struct {
	tunnel    *config.Proxy
	addr      net.Addr
	createdAt time.Time
}

type kcpServerSession struct {
	kcpConn *transport.KCPConn
	tunnel  *config.Proxy
	addr    net.Addr
	cancel  context.CancelFunc
}

type sessionState struct {
	sessionID  uint32
	tunnelName string
	clientAddr net.Addr
	clientConn net.Conn
	svcConn    net.Conn
	innerProto byte
}

func New(cfg *config.Config) *Server {
	ctx, cancel := context.WithCancel(context.Background())

	tunnels := make(map[string]*config.Proxy)
	for i := range cfg.Proxies {
		if cfg.Proxies[i].Type == "tunnel" {
			tunnels[cfg.Proxies[i].Name] = &cfg.Proxies[i]
		}
	}

	maxConn := int64(config.DefaultMaxConnections)
	if cfg.MaxConnections > 0 {
		maxConn = int64(cfg.MaxConnections)
	}

	return &Server{
		cfg:               cfg,
		tunnels:           tunnels,
		serviceConns:      make(map[string]net.Conn),
		udpClients:        make(map[string]*udpClient),
		kcpSessions:       make(map[uint32]*kcpServerSession),
		pendingKCP:        make(map[string]*pendingHandshake),
		sessions:          make(map[uint32]*sessionState),
		activeConnections: 0,
		maxConnections:    maxConn,
		ctx:               ctx,
		cancel:            cancel,
	}
}

func (s *Server) Start() error {
	needTCP := s.cfg.ListenProtocol == "tcp" || s.cfg.ListenProtocol == "all"
	needUDP := s.cfg.ListenProtocol == "udp" || s.cfg.ListenProtocol == "all"

	if needTCP {
		l, err := net.Listen("tcp", ":"+s.cfg.ListenPort)
		if err != nil {
			return fmt.Errorf("failed to listen TCP on port %s: %w", s.cfg.ListenPort, err)
		}
		s.tcpListener = l
		logger.Info("TCP listener started on :", s.cfg.ListenPort)
	}

	if needUDP {
		addr, err := net.ResolveUDPAddr("udp", ":"+s.cfg.ListenPort)
		if err != nil {
			return fmt.Errorf("failed to resolve UDP address: %w", err)
		}
		conn, err := net.ListenUDP("udp", addr)
		if err != nil {
			return fmt.Errorf("failed to listen UDP on port %s: %w", s.cfg.ListenPort, err)
		}
		s.udpConn = conn
		logger.Info("UDP listener started on :", s.cfg.ListenPort)
	}

	logger.Info("========================================")
	logger.Info("Config       : \"", s.cfg.Name, "\"")
	logger.Info("Listen port  : ", s.cfg.ListenPort)
	logger.Info("Transport    : ", s.cfg.ListenProtocol)
	logger.Info("Tunnels      : ", len(s.tunnels))
	for name, t := range s.tunnels {
		logger.Info("  - \"", name, "\" -> ", t.ServiceTarget, " (inner: ", t.AllowProtocol, ")")
	}
	logger.Info("========================================")

	if s.tcpListener != nil {
		s.wg.Add(1)
		go s.acceptTCP()
	}

	if s.udpConn != nil {
		s.wg.Add(1)
		go s.handleUDP()
	}

	// Start idle client cleanup goroutine
	s.wg.Add(1)
	go s.cleanupIdleClients()

	s.wg.Wait()
	return nil
}

func (s *Server) Shutdown() {
	s.cancel()
	if s.tcpListener != nil {
		s.tcpListener.Close()
	}
	if s.udpConn != nil {
		s.udpConn.Close()
	}

	s.sessionsMu.Lock()
	for _, ss := range s.sessions {
		if ss.svcConn != nil {
			ss.svcConn.Close()
		}
	}
	s.sessionsMu.Unlock()

	s.kcpSessionsMu.Lock()
	for _, ks := range s.kcpSessions {
		ks.kcpConn.Close()
	}
	s.kcpSessionsMu.Unlock()
}

func (s *Server) acceptTCP() {
	defer s.wg.Done()

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		conn, err := s.tcpListener.Accept()
		if err != nil {
			select {
			case <-s.ctx.Done():
				return
			default:
				logger.Error("TCP accept error: ", err)
				continue
			}
		}

		s.wg.Add(1)
		go s.handleTCPConnection(conn)
	}
}

func (s *Server) handleTCPConnection(conn net.Conn) {
	defer s.wg.Done()

	// Check connection limit (atomic operation)
	n := atomic.AddInt64(&s.activeConnections, 1)
	if n > s.maxConnections {
		atomic.AddInt64(&s.activeConnections, -1)
		conn.Close()
		logger.Warn("TCP connection rejected: limit reached (", s.maxConnections, ")")
		return
	}
	defer atomic.AddInt64(&s.activeConnections, -1)

	defer conn.Close()

	remoteAddr := conn.RemoteAddr().String()
	logger.Info("TCP transport connection from ", remoteAddr)

	h, err := s.processHandshake(conn, conn)
	if err != nil {
		logger.Error("Handshake failed from ", remoteAddr, ": ", err)
		return
	}

	logger.Info("[", h.TunnelName, "] Tunnel established | from=", remoteAddr, " | transport=TCP")

	s.processFrames(conn, conn, h.Tunnel)
}

func (s *Server) handleUDP() {
	defer s.wg.Done()

	buf := make([]byte, 65535)
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		n, addr, err := s.udpConn.ReadFrom(buf)
		if err != nil {
			select {
			case <-s.ctx.Done():
				return
			default:
				logger.Error("UDP read error: ", err)
				continue
			}
		}

		data := make([]byte, n)
		copy(data, buf[:n])

		// Improved packet type detection
		if s.isRawUDPFrame(data) {
			s.handleRawUDP(data, addr)
		} else {
			s.handleKCPPacket(data, addr)
		}
	}
}

// isRawUDPFrame performs reliable detection of raw UDP protocol frames vs KCP packets.
func (s *Server) isRawUDPFrame(data []byte) bool {
	if len(data) < protocol.HeaderSize {
		return false
	}

	if data[0] != protocol.Magic {
		return false
	}

	// Validate frame type
	frameType := data[1]
	validTypes := []byte{
		protocol.FrameHandshake,
		protocol.FrameHandshakeAck,
		protocol.FrameData,
		protocol.FrameClose,
	}

	typeValid := false
	for _, t := range validTypes {
		if frameType == t {
			typeValid = true
			break
		}
	}
	if !typeValid {
		return false
	}

	// Validate length consistency
	declaredLen := binary.BigEndian.Uint16(data[2:4])
	actualPayloadLen := len(data) - protocol.HeaderSize

	return int(declaredLen) == actualPayloadLen
}

func (s *Server) handleRawUDP(data []byte, addr net.Addr) {
	frame, err := protocol.ParseFrameFromDatagram(data)
	if err != nil {
		logger.Error("Failed to parse UDP frame from ", addr, ": ", err)
		return
	}

	clientKey := addr.String()

	if frame.Type == protocol.FrameHandshake {
		h, err := protocol.DecodeHandshake(frame.Payload)
		if err != nil {
			logger.Error("Failed to decode handshake from ", addr, ": ", err)
			return
		}

		tunnel, status := s.authenticate(h.TunnelName, h.Passwd)
		if status != protocol.StatusOK {
			ack := &protocol.HandshakeAck{StatusCode: status}
			ackFrame := &protocol.Frame{Type: protocol.FrameHandshakeAck, Payload: protocol.EncodeHandshakeAck(ack)}
			s.udpConn.WriteTo(protocol.EncodeFrame(ackFrame), addr)
			statusName := "Unknown"
			switch status {
			case protocol.StatusAuthFailed:
				statusName = "Auth Failed"
			case protocol.StatusTunnelNotFound:
				statusName = "Tunnel Not Found"
			}
			logger.Error("Auth failed | from=", addr, " | tunnel=", h.TunnelName, " | reason=", statusName)
			return
		}

		acceptedProto := h.TransportProto
		if acceptedProto == protocol.TransportAuto {
			acceptedProto = protocol.TransportUDP
		}

		ackProto := protocol.TransportUDP

		if acceptedProto == protocol.TransportTCP {
			s.pendingKCPMu.Lock()
			s.pendingKCP[clientKey] = &pendingHandshake{
				tunnel:    tunnel,
				addr:      addr,
				createdAt: time.Now(),
			}
			s.pendingKCPMu.Unlock()
			ackProto = protocol.TransportTCP
		}

		ack := &protocol.HandshakeAck{
			StatusCode:    protocol.StatusOK,
			AcceptedProto: ackProto,
		}
		ackFrame := &protocol.Frame{Type: protocol.FrameHandshakeAck, Payload: protocol.EncodeHandshakeAck(ack)}
		s.udpConn.WriteTo(protocol.EncodeFrame(ackFrame), addr)

		protoName := "UDP"
		if ackProto == protocol.TransportTCP {
			protoName = "KCP(TCP-in-UDP)"
		}
		logger.Info("[", h.TunnelName, "] Handshake OK | from=", addr, " | transport=", protoName)

		if acceptedProto == protocol.TransportUDP || acceptedProto == protocol.TransportAuto {
			// Check connection limit
			n := atomic.AddInt64(&s.activeConnections, 1)
			if n > s.maxConnections {
				atomic.AddInt64(&s.activeConnections, -1)
				logger.Warn("UDP client rejected: limit reached")
				return
			}

			s.udpClientsMu.Lock()
			s.udpClients[clientKey] = &udpClient{
				tunnel:     tunnel,
				addr:       addr,
				createdAt:  time.Now(),
				lastActive: time.Now(),
			}
			s.udpClientsMu.Unlock()
		}
		return
	}

	if frame.Type == protocol.FrameData {
		s.udpClientsMu.Lock()
		client, ok := s.udpClients[clientKey]
		if !ok {
			s.udpClientsMu.Unlock()
			logger.Warn("Data frame from unauthenticated client ", addr)
			return
		}
		client.lastActive = time.Now()
		s.udpClientsMu.Unlock()

		d, err := protocol.DecodeData(frame.Payload)
		if err != nil {
			logger.Error("Failed to decode data from ", addr, ": ", err)
			return
		}

		s.forwardData(client.tunnel, d, addr, nil)
		return
	}

	if frame.Type == protocol.FrameClose {
		s.udpClientsMu.RLock()
		client, ok := s.udpClients[clientKey]
		s.udpClientsMu.RUnlock()
		if !ok {
			return
		}

		cf, err := protocol.DecodeClose(frame.Payload)
		if err != nil {
			return
		}

		s.handleCloseFrame(cf)
		if cf.SessionID == 0 {
			s.udpClientsMu.Lock()
			delete(s.udpClients, clientKey)
			s.udpClientsMu.Unlock()
			atomic.AddInt64(&s.activeConnections, -1)
			logger.Info("[", client.tunnel.Name, "] Tunnel closed from ", addr)
		}
		return
	}
}

func (s *Server) handleKCPPacket(data []byte, addr net.Addr) {
	convID, ok := transport.ExtractConvID(data)
	if !ok {
		return
	}

	s.kcpSessionsMu.Lock()
	ks, ok := s.kcpSessions[convID]
	if !ok {
		clientKey := addr.String()
		s.pendingKCPMu.Lock()
		pending, exists := s.pendingKCP[clientKey]
		if exists {
			delete(s.pendingKCP, clientKey)
		}
		s.pendingKCPMu.Unlock()

		if !exists {
			s.kcpSessionsMu.Unlock()
			return
		}

		// Check connection limit
		n := atomic.AddInt64(&s.activeConnections, 1)
		if n > s.maxConnections {
			atomic.AddInt64(&s.activeConnections, -1)
			s.kcpSessionsMu.Unlock()
			logger.Warn("KCP session rejected: limit reached (", s.maxConnections, ")")
			return
		}

		kcpConn := transport.NewKCPConn(convID, s.udpConn, addr, s.udpConn.LocalAddr())
		ks = &kcpServerSession{
			kcpConn: kcpConn,
			tunnel:  pending.tunnel,
			addr:    addr,
		}
		s.kcpSessions[convID] = ks

		logger.Info("[", pending.tunnel.Name, "] KCP session (TCP-in-UDP) established | conv=", convID, " | from=", addr)

		s.wg.Add(1)
		go s.handleKCPSession(ks)
	}
	s.kcpSessionsMu.Unlock()

	ks.kcpConn.Input(data)
}

func (s *Server) handleKCPSession(ks *kcpServerSession) {
	defer s.wg.Done()
	defer ks.kcpConn.Close()
	defer func() {
		s.kcpSessionsMu.Lock()
		delete(s.kcpSessions, ks.kcpConn.ConvID())
		s.kcpSessionsMu.Unlock()
		atomic.AddInt64(&s.activeConnections, -1)
	}()

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		frame, err := protocol.ReadFrame(ks.kcpConn)
		if err != nil {
			if !ks.kcpConn.IsClosed() {
				logger.Error("KCP frame read error (conv=", ks.kcpConn.ConvID(), "): ", err)
			}
			return
		}

		if frame.Type == protocol.FrameData {
			d, err := protocol.DecodeData(frame.Payload)
			if err != nil {
				logger.Error("Failed to decode KCP data: ", err)
				continue
			}
			s.forwardData(ks.tunnel, d, ks.addr, ks.kcpConn)
		} else if frame.Type == protocol.FrameClose {
			cf, err := protocol.DecodeClose(frame.Payload)
			if err != nil {
				continue
			}
			s.handleCloseFrame(cf)
		}
	}
}

type handshakeResult struct {
	Tunnel     *config.Proxy
	TunnelName string
}

func (s *Server) processHandshake(conn net.Conn, responder net.Conn) (*handshakeResult, error) {
	frame, err := protocol.ReadFrame(conn)
	if err != nil {
		return nil, fmt.Errorf("failed to read handshake: %w", err)
	}

	if frame.Type != protocol.FrameHandshake {
		return nil, fmt.Errorf("expected handshake frame, got type 0x%02x", frame.Type)
	}

	h, err := protocol.DecodeHandshake(frame.Payload)
	if err != nil {
		return nil, fmt.Errorf("failed to decode handshake: %w", err)
	}

	tunnel, status := s.authenticate(h.TunnelName, h.Passwd)

	acceptedProto := h.TransportProto
	if acceptedProto == protocol.TransportAuto || acceptedProto == protocol.TransportTCP {
		acceptedProto = protocol.TransportTCP
	}

	ack := &protocol.HandshakeAck{
		StatusCode:    status,
		AcceptedProto: acceptedProto,
	}

	ackPayload := protocol.EncodeHandshakeAck(ack)
	ackFrame := &protocol.Frame{Type: protocol.FrameHandshakeAck, Payload: ackPayload}
	if err := protocol.WriteFrame(responder, ackFrame); err != nil {
		return nil, fmt.Errorf("failed to send handshake ack: %w", err)
	}

	if status != protocol.StatusOK {
		return nil, fmt.Errorf("handshake rejected: status=%d", status)
	}

	return &handshakeResult{Tunnel: tunnel, TunnelName: h.TunnelName}, nil
}

func (s *Server) authenticate(tunnelName, passwd string) (*config.Proxy, byte) {
	tunnel, ok := s.tunnels[tunnelName]
	if !ok {
		return nil, protocol.StatusTunnelNotFound
	}

	if subtle.ConstantTimeCompare([]byte(passwd), []byte(tunnel.Passwd)) == 1 {
		return tunnel, protocol.StatusOK
	}

	if s.cfg.AdminPasswd != "" && subtle.ConstantTimeCompare([]byte(passwd), []byte(s.cfg.AdminPasswd)) == 1 {
		return tunnel, protocol.StatusOK
	}

	return nil, protocol.StatusAuthFailed
}

func (s *Server) processFrames(reader ioReader, writer net.Conn, tunnel *config.Proxy) {
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		frame, err := protocol.ReadFrame(reader)
		if err != nil {
			logger.Info("[", tunnel.Name, "] Frame read error: ", err)
			return
		}

		switch frame.Type {
		case protocol.FrameData:
			d, err := protocol.DecodeData(frame.Payload)
			if err != nil {
				logger.Error("Failed to decode data: ", err)
				continue
			}
			s.forwardData(tunnel, d, nil, writer)

		case protocol.FrameClose:
			cf, err := protocol.DecodeClose(frame.Payload)
			if err != nil {
				continue
			}
			s.handleCloseFrame(cf)
			if cf.SessionID == 0 {
				return
			}

		default:
			logger.Warn("Unknown frame type 0x", fmt.Sprintf("%02x", frame.Type))
		}
	}
}

func (s *Server) forwardData(tunnel *config.Proxy, d *protocol.Data, clientAddr net.Addr, clientConn net.Conn) {
	if tunnel.AllowProtocol == "udp" && d.InnerProto != protocol.InnerProtoUDP {
		return
	}
	if tunnel.AllowProtocol == "tcp" && d.InnerProto != protocol.InnerProtoTCP {
		return
	}

	s.sessionsMu.Lock()
	ss, ok := s.sessions[d.SessionID]
	if !ok {
		svcConn, err := s.getServiceConn(tunnel, d.InnerProto)
		if err != nil {
			logger.Error("[", tunnel.Name, "] Failed to connect to ", tunnel.ServiceTarget, ": ", err)
			s.sessionsMu.Unlock()
			s.sendClose(clientConn, clientAddr, d.SessionID, protocol.CloseTargetUnreachable)
			return
		}

		ss = &sessionState{
			sessionID:  d.SessionID,
			tunnelName: tunnel.Name,
			clientAddr: clientAddr,
			clientConn: clientConn,
			svcConn:    svcConn,
			innerProto: d.InnerProto,
		}
		s.sessions[d.SessionID] = ss

		transportName := "UDP"
		if clientConn != nil && clientAddr == nil {
			transportName = "TCP"
		} else if clientConn != nil && clientAddr != nil {
			transportName = "KCP"
		}
		innerName := "UDP"
		if d.InnerProto == protocol.InnerProtoTCP {
			innerName = "TCP"
		}
		logger.Info("[", tunnel.Name, "] New session | id=0x", fmt.Sprintf("%08X", d.SessionID),
			" | mode=", innerName, "-in-", transportName, " | target=", tunnel.ServiceTarget)

		s.wg.Add(1)
		go s.readFromService(ss)
	}
	s.sessionsMu.Unlock()

	if ss.svcConn != nil && len(d.InnerPayload) > 0 {
		if _, err := ss.svcConn.Write(d.InnerPayload); err != nil {
			logger.Error("[", tunnel.Name, "] Write to ", tunnel.ServiceTarget, " failed: ", err)

			// Send close frame and clean up session
			s.sendClose(clientConn, clientAddr, d.SessionID, protocol.CloseTargetUnreachable)
			s.sessionsMu.Lock()
			delete(s.sessions, d.SessionID)
			s.sessionsMu.Unlock()
			if ss.svcConn != nil {
				ss.svcConn.Close()
			}
			return
		}
	}
}

func (s *Server) readFromService(ss *sessionState) {
	defer s.wg.Done()
	defer func() {
		s.sessionsMu.Lock()
		delete(s.sessions, ss.sessionID)
		s.sessionsMu.Unlock()
		if ss.svcConn != nil {
			ss.svcConn.Close()
		}
	}()

	buf := make([]byte, 65535)
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		n, err := ss.svcConn.Read(buf)
		if err != nil {
			return
		}

		payload := buf[:n]
		maxChunk := protocol.MaxFramePayloadSize - 5 // subtract Data header (sessionID + proto)

		if ss.clientAddr != nil {
			// UDP datagram path: datagrams are atomic and cannot be split.
			// Drop oversized datagrams instead of chunking.
			if len(payload) > maxChunk {
				logger.Warn("[", ss.tunnelName, "] Dropping oversized UDP datagram (", len(payload), " bytes, limit ", maxChunk, ")")
				continue
			}

			data := &protocol.Data{
				SessionID:    ss.sessionID,
				InnerProto:   ss.innerProto,
				InnerPayload: payload,
			}
			frame := &protocol.Frame{
				Type:    protocol.FrameData,
				Payload: protocol.EncodeData(data),
			}
			if _, err := s.udpConn.WriteTo(protocol.EncodeFrame(frame), ss.clientAddr); err != nil {
				logger.Debug("UDP write to client failed: ", err)
			}
			continue
		}

		// TCP/KCP stream path: chunk large data into multiple frames
		for len(payload) > 0 {
			chunkSize := len(payload)
			if chunkSize > maxChunk {
				chunkSize = maxChunk
			}

			data := &protocol.Data{
				SessionID:    ss.sessionID,
				InnerProto:   ss.innerProto,
				InnerPayload: payload[:chunkSize],
			}

			frame := &protocol.Frame{
				Type:    protocol.FrameData,
				Payload: protocol.EncodeData(data),
			}

			if err := protocol.WriteFrame(ss.clientConn, frame); err != nil {
				return
			}

			payload = payload[chunkSize:]
		}
	}
}

func (s *Server) getServiceConn(tunnel *config.Proxy, innerProto byte) (net.Conn, error) {
	switch innerProto {
	case protocol.InnerProtoUDP:
		return net.Dial("udp", tunnel.ServiceTarget)
	case protocol.InnerProtoTCP:
		return net.Dial("tcp", tunnel.ServiceTarget)
	default:
		return nil, fmt.Errorf("unknown inner protocol: %d", innerProto)
	}
}

func (s *Server) handleCloseFrame(cf *protocol.CloseFrame) {
	s.sessionsMu.Lock()
	defer s.sessionsMu.Unlock()

	if ss, ok := s.sessions[cf.SessionID]; ok {
		if ss.svcConn != nil {
			ss.svcConn.Close()
		}
		delete(s.sessions, cf.SessionID)
		reasonName := "Normal"
		switch cf.Reason {
		case protocol.CloseTimeout:
			reasonName = "Timeout"
		case protocol.CloseTargetUnreachable:
			reasonName = "Target Unreachable"
		case protocol.CloseProtocolError:
			reasonName = "Protocol Error"
		}
		logger.Info("[", ss.tunnelName, "] Session closed | id=0x", fmt.Sprintf("%08X", cf.SessionID), " | reason=", reasonName)
	}
}

func (s *Server) sendClose(conn net.Conn, addr net.Addr, sessionID uint32, reason byte) {
	cf := &protocol.CloseFrame{SessionID: sessionID, Reason: reason}
	f := &protocol.Frame{Type: protocol.FrameClose, Payload: protocol.EncodeClose(cf)}

	if conn != nil {
		protocol.WriteFrame(conn, f)
	} else if addr != nil {
		s.udpConn.WriteTo(protocol.EncodeFrame(f), addr)
	}
}

type ioReader interface {
	Read([]byte) (int, error)
}

func (s *Server) cleanupIdleClients() {
	defer s.wg.Done()

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			idleTimeout := 10 * time.Minute

			// Clean up idle UDP clients
			s.udpClientsMu.Lock()
			for key, client := range s.udpClients {
				if now.Sub(client.lastActive) > idleTimeout {
					delete(s.udpClients, key)
					atomic.AddInt64(&s.activeConnections, -1)
					logger.Debug("Cleaned up idle UDP client: ", key)
				}
			}
			s.udpClientsMu.Unlock()

			// Clean up stale pending KCP handshakes
			s.pendingKCPMu.Lock()
			for key, pending := range s.pendingKCP {
				if now.Sub(pending.createdAt) > 30*time.Second {
					delete(s.pendingKCP, key)
					logger.Debug("Cleaned up stale pending KCP: ", key)
				}
			}
			s.pendingKCPMu.Unlock()
		}
	}
}
