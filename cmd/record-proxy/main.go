// record-proxy is a TCP tee for capturing CAS wire traffic.
//
//	go run ./cmd/record-proxy -listen :7777 -target broker-host:33000
//
// Frames flowing each way are hex-dumped to stdout with direction markers.
package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
)

var mu sync.Mutex

type tap struct {
	dir string
	w   io.Writer
}

func (t *tap) Write(p []byte) (int, error) {
	mu.Lock()
	fmt.Fprintf(os.Stdout, "%s %d bytes\n%s\n", t.dir, len(p), hex.Dump(p))
	mu.Unlock()
	return t.w.Write(p)
}

func main() {
	listen := flag.String("listen", ":7777", "listen address")
	target := flag.String("target", "", "broker address host:port")
	flag.Parse()
	if *target == "" {
		log.Fatal("-target required")
	}
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("proxying %s -> %s", *listen, *target)
	for {
		c, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go func() {
			defer c.Close()
			up, err := net.Dial("tcp", *target)
			if err != nil {
				log.Print(err)
				return
			}
			defer up.Close()
			done := make(chan struct{}, 2)
			go func() { io.Copy(&tap{dir: ">>", w: up}, c); done <- struct{}{} }()
			go func() { io.Copy(&tap{dir: "<<", w: c}, up); done <- struct{}{} }()
			<-done
		}()
	}
}
