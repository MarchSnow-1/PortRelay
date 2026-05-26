package client

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"portrelay/config"
)

type DirectClient struct {
	proxy  *config.Proxy
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewDirectClient(proxy *config.Proxy) *DirectClient {
	ctx, cancel := context.WithCancel(context.Background())
	return &DirectClient{
		proxy:  proxy,
		ctx:    ctx,
		cancel: cancel,
	}
}

func (d *DirectClient) Start() error {
	log.Printf("[Direct \"%s\"] Starting %s forwarder: %s -> %s",
		d.proxy.Name, d.proxy.Protocol, d.proxy.Listen, d.proxy.Target)

	switch d.proxy.Protocol {
	case "tcp":
		return d.startTCP()
	case "udp":
		return d.startUDP()
	case "all":
		d.wg.Add(2)
		go func() { defer d.wg.Done(); d.startTCP() }()
		go func() { defer d.wg.Done(); d.startUDP() }()
		d.wg.Wait()
		return nil
	default:
		return fmt.Errorf("unsupported protocol: %s", d.proxy.Protocol)
	}
}

func (d *DirectClient) Stop() {
	d.cancel()
}

func (d *DirectClient) startTCP() error {
	l, err := net.Listen("tcp", d.proxy.Listen)
	if err != nil {
		return fmt.Errorf("failed to listen TCP on %s: %w", d.proxy.Listen, err)
	}
	defer l.Close()

	log.Printf("[Direct \"%s\"] TCP listener on %s", d.proxy.Name, d.proxy.Listen)

	for {
		select {
		case <-d.ctx.Done():
			return nil
		default:
		}

		conn, err := l.Accept()
		if err != nil {
			select {
			case <-d.ctx.Done():
				return nil
			default:
				log.Printf("[Direct \"%s\"] TCP accept error: %v", d.proxy.Name, err)
				continue
			}
		}

		d.wg.Add(1)
		go d.handleTCPConn(conn)
	}
}

func (d *DirectClient) handleTCPConn(localConn net.Conn) {
	defer d.wg.Done()
	defer localConn.Close()

	remoteConn, err := net.DialTimeout("tcp", d.proxy.Target, 10*time.Second)
	if err != nil {
		log.Printf("[Direct \"%s\"] Failed to connect to target %s: %v", d.proxy.Name, d.proxy.Target, err)
		return
	}
	defer remoteConn.Close()

	ctx, cancel := context.WithCancel(d.ctx)
	defer cancel()

	go func() {
		io.Copy(remoteConn, localConn)
		cancel()
	}()

	go func() {
		io.Copy(localConn, remoteConn)
		cancel()
	}()

	<-ctx.Done()
}

func (d *DirectClient) startUDP() error {
	addr, err := net.ResolveUDPAddr("udp", d.proxy.Listen)
	if err != nil {
		return fmt.Errorf("failed to resolve UDP address %s: %w", d.proxy.Listen, err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen UDP on %s: %w", d.proxy.Listen, err)
	}
	defer conn.Close()

	log.Printf("[Direct \"%s\"] UDP listener on %s", d.proxy.Name, d.proxy.Listen)

	raddr, err := net.ResolveUDPAddr("udp", d.proxy.Target)
	if err != nil {
		return fmt.Errorf("failed to resolve target UDP address %s: %w", d.proxy.Target, err)
	}

	// Map local client address -> remote connection
	clients := make(map[string]*udpForwardSession)
	var mu sync.Mutex

	buf := make([]byte, 65535)
	for {
		select {
		case <-d.ctx.Done():
			return nil
		default:
		}

		n, clientAddr, err := conn.ReadFrom(buf)
		if err != nil {
			select {
			case <-d.ctx.Done():
				return nil
			default:
				log.Printf("[Direct \"%s\"] UDP read error: %v", d.proxy.Name, err)
				continue
			}
		}

		clientKey := clientAddr.String()

		mu.Lock()
		sess, ok := clients[clientKey]
		if !ok {
			remoteConn, err := net.DialUDP("udp", nil, raddr)
			if err != nil {
				log.Printf("[Direct \"%s\"] Failed to dial target UDP: %v", d.proxy.Name, err)
				mu.Unlock()
				continue
			}

			sess = &udpForwardSession{
				remoteConn: remoteConn,
				clientAddr: clientAddr,
			}
			clients[clientKey] = sess

			d.wg.Add(1)
			go d.handleUDPResponses(conn, remoteConn, clientAddr, clientKey, &mu, clients)
		}
		mu.Unlock()

		sess.remoteConn.Write(buf[:n])
	}
}

type udpForwardSession struct {
	remoteConn *net.UDPConn
	clientAddr net.Addr
}

func (d *DirectClient) handleUDPResponses(localConn *net.UDPConn, remoteConn *net.UDPConn, clientAddr net.Addr, clientKey string, mu *sync.Mutex, clients map[string]*udpForwardSession) {
	defer d.wg.Done()
	defer remoteConn.Close()
	defer func() {
		mu.Lock()
		delete(clients, clientKey)
		mu.Unlock()
	}()

	buf := make([]byte, 65535)
	for {
		select {
		case <-d.ctx.Done():
			return
		default:
		}

		remoteConn.SetReadDeadline(time.Now().Add(60 * time.Second))
		n, _, err := remoteConn.ReadFrom(buf)
		if err != nil {
			return
		}

		localConn.WriteTo(buf[:n], clientAddr)
	}
}
