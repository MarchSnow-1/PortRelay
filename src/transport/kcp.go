package transport

import (
	"encoding/binary"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/xtaci/kcp-go/v5"
)

const (
	kcpMTU         = 1400
	kcpSendWindow  = 128
	kcpRecvWindow  = 128
	kcpNoDelay     = 1
	kcpInterval    = 10
	kcpResend      = 2
	kcpNC          = 1
	kcpReadBufSize = 4096
)

// KCPConn wraps a KCP conversation, implementing net.Conn.
type KCPConn struct {
	convID     uint32
	kcp        *kcp.KCP
	conn       net.PacketConn
	remoteAddr net.Addr
	localAddr  net.Addr

	readBuf   []byte
	readPos   int
	readLen   int
	readMu    sync.Mutex
	readCond  *sync.Cond

	writeMu sync.Mutex

	closed    bool
	closeMu   sync.RWMutex
	closeCh   chan struct{}
	closeOnce sync.Once

	updateStop chan struct{}
}

// DialKCP creates a KCP connection to a remote address.
func DialKCP(address string) (*KCPConn, error) {
	raddr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return nil, err
	}

	conn, err := net.ListenUDP("udp", nil)
	if err != nil {
		return nil, err
	}

	convID := rand.Uint32()

	return newKCPConn(convID, conn, raddr, conn.LocalAddr()), nil
}

// NewKCPConn creates a KCPConn from an existing PacketConn and remote address.
func NewKCPConn(convID uint32, conn net.PacketConn, remoteAddr net.Addr, localAddr net.Addr) *KCPConn {
	return newKCPConn(convID, conn, remoteAddr, localAddr)
}

func newKCPConn(convID uint32, conn net.PacketConn, remoteAddr net.Addr, localAddr net.Addr) *KCPConn {
	kc := &KCPConn{
		convID:     convID,
		conn:       conn,
		remoteAddr: remoteAddr,
		localAddr:  localAddr,
		readBuf:    make([]byte, kcpReadBufSize),
		closeCh:    make(chan struct{}),
		updateStop: make(chan struct{}),
	}

	kc.readCond = sync.NewCond(&kc.readMu)

	output := func(buf []byte, size int) {
		if kc.IsClosed() {
			return
		}
		kc.conn.WriteTo(buf[:size], kc.remoteAddr)
	}

	kc.kcp = kcp.NewKCP(convID, output)
	kc.kcp.SetMtu(kcpMTU)
	kc.kcp.WndSize(kcpSendWindow, kcpRecvWindow)
	kc.kcp.NoDelay(kcpNoDelay, kcpInterval, kcpResend, kcpNC)

	go kc.updateLoop()

	return kc
}

func (k *KCPConn) updateLoop() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-k.closeCh:
			return
		case <-k.updateStop:
			return
		case <-ticker.C:
			k.closeMu.RLock()
			if k.closed {
				k.closeMu.RUnlock()
				return
			}
			k.closeMu.RUnlock()

			k.kcp.Update()
		}
	}
}

// Input feeds a raw KCP packet into the KCP instance.
// Called when a KCP packet is received from the network.
func (k *KCPConn) Input(data []byte) {
	if k.IsClosed() {
		return
	}

	k.kcp.Input(data, kcp.IKCP_PACKET_REGULAR, false)

	// Signal readers that new data might be available
	k.readCond.Broadcast()
}

// Read reads data from the KCP stream. Implements io.Reader.
func (k *KCPConn) Read(b []byte) (int, error) {
	k.readMu.Lock()
	defer k.readMu.Unlock()

	for {
		if k.IsClosed() {
			return 0, net.ErrClosed
		}

		// Try to receive from KCP
		n := k.kcp.Recv(b)
		if n > 0 {
			return n, nil
		}

		// Check if there's pending data in the internal read buffer
		if k.readPos < k.readLen {
			n := copy(b, k.readBuf[k.readPos:k.readLen])
			k.readPos += n
			if k.readPos >= k.readLen {
				k.readPos = 0
				k.readLen = 0
			}
			if n > 0 {
				return n, nil
			}
		}

		// Wait for new data
		k.readCond.Wait()
	}
}

// Write writes data to the KCP stream. Implements io.Writer.
func (k *KCPConn) Write(b []byte) (int, error) {
	if k.IsClosed() {
		return 0, net.ErrClosed
	}

	k.writeMu.Lock()
	defer k.writeMu.Unlock()

	n := k.kcp.Send(b)
	if n < 0 {
		return 0, net.ErrClosed
	}

	return len(b), nil
}

// Close closes the KCP connection.
func (k *KCPConn) Close() error {
	k.closeOnce.Do(func() {
		k.closeMu.Lock()
		k.closed = true
		k.closeMu.Unlock()

		close(k.closeCh)

		// Wake up any waiting readers
		k.readCond.Broadcast()
	})

	return nil
}

// IsClosed returns whether the connection is closed.
func (k *KCPConn) IsClosed() bool {
	k.closeMu.RLock()
	defer k.closeMu.RUnlock()
	return k.closed
}

// LocalAddr returns the local network address.
func (k *KCPConn) LocalAddr() net.Addr {
	return k.localAddr
}

// RemoteAddr returns the remote network address.
func (k *KCPConn) RemoteAddr() net.Addr {
	return k.remoteAddr
}

// SetDeadline implements net.Conn. Not supported for KCP.
func (k *KCPConn) SetDeadline(t time.Time) error {
	return nil
}

// SetReadDeadline implements net.Conn. Not supported for KCP.
func (k *KCPConn) SetReadDeadline(t time.Time) error {
	return nil
}

// SetWriteDeadline implements net.Conn. Not supported for KCP.
func (k *KCPConn) SetWriteDeadline(t time.Time) error {
	return nil
}

// ConvID returns the KCP conversation ID.
func (k *KCPConn) ConvID() uint32 {
	return k.convID
}

// ExtractConvID extracts the conversation ID from a raw KCP packet.
func ExtractConvID(data []byte) (uint32, bool) {
	if len(data) < 4 {
		return 0, false
	}
	return binary.BigEndian.Uint32(data[0:4]), true
}
