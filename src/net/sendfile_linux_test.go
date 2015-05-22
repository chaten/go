package net_test

import (
	"bytes"
	"io"
	"net"
	"sync"
	"testing"
)

//Tests how fast it is to copy data directly from one socket to another
func BenchmarkSocketProxy(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()
	bufLen := 1024 * 1024 * 128
	b.SetBytes(int64(bufLen))
	buf := make([]byte, bufLen)
	for i := range buf {
		buf[i] = byte(i)
	}

	writeBuf := bytes.Buffer{}
	writeBuf.Grow(bufLen)

	for i := 0; i < b.N; i += 1 {
		func() {
			proxyListener, err := net.ListenTCP("tcp", &net.TCPAddr{})
			if err != nil {
				b.Fatal(err)
			}
			defer proxyListener.Close()
			proxyAddr, _ := proxyListener.Addr().(*net.TCPAddr)

			serverListener, err := net.ListenTCP("tcp", &net.TCPAddr{})
			if err != nil {
				b.Fatal(err)
			}
			defer serverListener.Close()
			serverAddr, _ := serverListener.Addr().(*net.TCPAddr)

			var wg sync.WaitGroup
			wg.Add(3)
			//Client Goroutine
			go func() {
				defer wg.Done()
				conn, err := net.DialTCP("tcp", nil, proxyAddr)
				if err != nil {
					b.Fatal(err)
				}
				defer conn.Close()
				_, err = io.Copy(conn, bytes.NewReader(buf))
				if err != nil {
					b.Fatal(err)
				}
			}()

			//Proxy Goroutine
			go func() {
				defer wg.Done()
				lconn, err := proxyListener.AcceptTCP()
				if err != nil {
					b.Fatal(err)
				}
				defer lconn.Close()
				rconn, err := net.DialTCP("tcp", nil, serverAddr)
				if err != nil {
					b.Fatal(err)
				}
				defer rconn.Close()
				b.StartTimer()
				defer b.StopTimer()
				proxy(lconn, rconn)
			}()

			//Server Goroutine
			go func() {
				defer wg.Done()
				conn, err := serverListener.AcceptTCP()
				if err != nil {
					b.Fatal(err)
				}
				defer conn.Close()
				writeBuf.Reset()
				_, err = io.Copy(&writeBuf, conn)
				if err != nil {
					b.Fatal(err)
				}
			}()
			wg.Wait()

		}()

	}
	for i := 0; i < bufLen; i += 1 {
		if buf[i] != writeBuf.Bytes()[i] {
			b.Errorf("Incorrect value written at index %d; was %d, wanted %d", i, writeBuf.Bytes()[i], buf[i])
		}
	}
}

func proxy(l, r *net.TCPConn) {
	defer l.Close()
	defer r.Close()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		defer l.CloseWrite()
		defer r.CloseRead()
		io.Copy(l, r)
	}()
	go func() {
		defer wg.Done()
		defer l.CloseRead()
		defer r.CloseWrite()
		io.Copy(r, l)
	}()
	wg.Wait()
}
