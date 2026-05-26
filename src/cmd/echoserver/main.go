package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
)

func main() {
	mode := flag.String("mode", "", "echo server mode: tcp or udp")
	listen := flag.String("listen", "", "listen address")
	flag.Parse()

	if *mode == "" || *listen == "" {
		fmt.Fprintf(os.Stderr, "Usage: %s --mode <tcp|udp> --listen <addr:port>\n", os.Args[0])
		os.Exit(1)
	}

	switch *mode {
	case "tcp":
		runTCPEcho(*listen)
	case "udp":
		runUDPEcho(*listen)
	default:
		log.Fatalf("Unknown mode: %s", *mode)
	}
}

func runTCPEcho(addr string) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("TCP listen failed: %v", err)
	}
	defer l.Close()
	log.Printf("TCP echo server listening on %s", addr)

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Printf("Accept error: %v", err)
			continue
		}
		go func(c net.Conn) {
			defer c.Close()
			buf := make([]byte, 4096)
			for {
				n, err := c.Read(buf)
				if err != nil {
					if err != io.EOF {
						log.Printf("TCP read error: %v", err)
					}
					return
				}
				log.Printf("TCP echo: received %d bytes from %s", n, c.RemoteAddr().String())
				if _, err := c.Write(buf[:n]); err != nil {
					log.Printf("TCP write error: %v", err)
					return
				}
				log.Printf("TCP echo: sent %d bytes back", n)
			}
		}(conn)
	}
}

func runUDPEcho(addr string) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		log.Fatalf("UDP resolve failed: %v", err)
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		log.Fatalf("UDP listen failed: %v", err)
	}
	defer conn.Close()
	log.Printf("UDP echo server listening on %s", addr)

	buf := make([]byte, 65535)
	for {
		n, remoteAddr, err := conn.ReadFrom(buf)
		if err != nil {
			log.Printf("UDP read error: %v", err)
			continue
		}
		log.Printf("UDP echo: received %d bytes from %s", n, remoteAddr.String())
		if _, err := conn.WriteTo(buf[:n], remoteAddr); err != nil {
			log.Printf("UDP write error: %v", err)
			continue
		}
		log.Printf("UDP echo: sent %d bytes back", n)
	}
}
