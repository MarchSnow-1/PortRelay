package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"time"
)

func main() {
	mode := flag.String("mode", "", "test mode: tcp or udp")
	target := flag.String("target", "", "target address")
	flag.Parse()

	if *mode == "" || *target == "" {
		fmt.Fprintf(os.Stderr, "Usage: %s --mode <tcp|udp> --target <addr:port>\n", os.Args[0])
		os.Exit(1)
	}

	msg := []byte("HELLO_PORTRELAY_TEST")

	switch *mode {
	case "tcp":
		testTCP(*target, msg)
	case "udp":
		testUDP(*target, msg)
	}
}

func testTCP(target string, msg []byte) {
	conn, err := net.DialTimeout("tcp", target, 5*time.Second)
	if err != nil {
		log.Fatalf("FAIL: TCP dial failed: %v", err)
	}
	defer conn.Close()
	log.Printf("TCP connected to %s", target)

	if _, err := conn.Write(msg); err != nil {
		log.Fatalf("FAIL: TCP write failed: %v", err)
	}
	log.Printf("Sent: %s", string(msg))

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		log.Fatalf("FAIL: TCP read failed: %v", err)
	}

	if string(buf[:n]) == string(msg) {
		log.Printf("PASS: Received echo: %s", string(buf[:n]))
	} else {
		log.Fatalf("FAIL: Expected %q, got %q", string(msg), string(buf[:n]))
	}
}

func testUDP(target string, msg []byte) {
	raddr, err := net.ResolveUDPAddr("udp", target)
	if err != nil {
		log.Fatalf("FAIL: UDP resolve failed: %v", err)
	}

	conn, err := net.DialUDP("udp", nil, raddr)
	if err != nil {
		log.Fatalf("FAIL: UDP dial failed: %v", err)
	}
	defer conn.Close()
	log.Printf("UDP connected to %s", target)

	if _, err := conn.Write(msg); err != nil {
		log.Fatalf("FAIL: UDP write failed: %v", err)
	}
	log.Printf("Sent: %s", string(msg))

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		log.Fatalf("FAIL: UDP read failed: %v", err)
	}

	if string(buf[:n]) == string(msg) {
		log.Printf("PASS: Received echo: %s", string(buf[:n]))
	} else {
		log.Fatalf("FAIL: Expected %q, got %q", string(msg), string(buf[:n]))
	}
}
