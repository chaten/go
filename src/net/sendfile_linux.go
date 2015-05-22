// Copyright 2011 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package net

import (
	"io"
	"os"
	"syscall"
)

// maxSendfileSize is the largest chunk size we ask the kernel to copy
// at a time.
// If this is greater than the kernel's max pipe buffer size, a deadlock can occur.
// This is 64KB on Linux 2.6 and above
const maxSendfileSize int = 1024 * 64

//TODO: Add splice constants to syscall package?
const _SPLICE_F_MORE int = 0x4

// sendFile copies the contents of r to c using the splice
// system call to minimize copies.
//
// if handled == true, sendFile returns the number of bytes copied and any
// non-EOF error.
//
// if handled == false, sendFile performed no work.
func sendFile(c *netFD, r io.Reader) (written int64, err error, handled bool) {
	var remain int64 = 1 << 62 // by default, copy until EOF

	lr, ok := r.(*io.LimitedReader)
	if ok {
		remain, r = lr.N, lr.R
		if remain <= 0 {
			return 0, nil, true
		}
	}
	src, ok := spliceableReader(r, c)
	if !ok {
		return 0, nil, false
	}
	dst := &spliceable{
		c:      c,
		fd:     c.sysfd,
		lock:   c.writeLock,
		unlock: c.writeUnlock,
		wait:   c.pd.WaitWrite,
	}

	if err := dst.lock(); err != nil {
		return 0, err, true
	}
	defer dst.unlock()
	if err := src.lock(); err != nil {
		return 0, err, true
	}
	defer src.unlock()

	rPipe, wPipe, err := os.Pipe()
	if err != nil {
		return 0, &OpError{"pipe", c.net, c.raddr, err}, false
	}
	defer rPipe.Close()
	defer wPipe.Close()
	rPipeFd, wPipeFd := int(rPipe.Fd()), int(wPipe.Fd())
	pipeLen := 0
	for remain > 0 || pipeLen > 0 {
		toRead := maxSendfileSize
		if int64(toRead) >= remain {
			toRead = int(remain)
		}
		// if we have stuff to read and we won't overflow pipeLen by reading more
		if pipeLen+toRead > pipeLen && pipeLen+toRead <= maxSendfileSize {
			n, rerr := syscall.Splice(src.fd, nil, wPipeFd, nil, toRead, _SPLICE_F_MORE)
			if n > 0 {
				remain -= n
				pipeLen += int(n)
			}
			if n == 0 && rerr == nil {
				break
			}
			rerr = handleSpliceErr(src, rerr)
			if rerr != nil {
				err = rerr
				break
			}
		}
		if pipeLen > 0 {
			n, werr := syscall.Splice(rPipeFd, nil, dst.fd, nil, pipeLen, _SPLICE_F_MORE)
			if n > 0 {
				written += n
				pipeLen -= int(n)
			}
			if n == 0 && werr == nil {
				break
			}
			werr = handleSpliceErr(dst, werr)
			if werr != nil {
				err = werr
				break
			}
		}
	}
	if lr != nil {
		lr.N = remain
	}
	return written, err, written > 0
}

type spliceable struct {
	//The connection any OpErrors should be associated with
	c      *netFD
	fd     int
	lock   func() error
	unlock func()
	wait   func() error
}

func spliceableReader(r io.Reader, c *netFD) (*spliceable, bool) {
	if f, ok := r.(*os.File); ok {
		doNothing := func() error { return nil }
		return &spliceable{
			c:      c,
			fd:     int(f.Fd()),
			lock:   doNothing,
			unlock: func() {},
			wait:   doNothing,
		}, true
	}
	if conn, ok := r.(*TCPConn); ok {
		return &spliceable{
			c:      conn.fd,
			fd:     conn.fd.sysfd,
			lock:   conn.fd.readLock,
			unlock: conn.fd.readUnlock,
			wait:   conn.fd.pd.WaitRead,
		}, true
	}
	return nil, false
}

func handleSpliceErr(s *spliceable, err error) error {
	if err == syscall.EAGAIN {
		err = s.wait()
	}
	if err != nil {
		// This includes syscall.ENOSYS (no kernel
		// support) and syscall.EINVAL (fd types which
		// don't implement sendfile together)
		err = &OpError{"s", s.c.net, s.c.raddr, err}
	}
	return err
}
