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
const maxSendfileSize int = 1024 * 16
//TODO: Add splice constants to syscall package?
const splice_f_more int = 0x4

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
	src, ok := fd(r)
	if !ok {
		return 0, nil, false
	}

	if err := c.writeLock(); err != nil {
		return 0, err, true
	}
	defer c.writeUnlock()

	dst := c.sysfd
	rPipe, wPipe, err := os.Pipe()
	if err != nil {
		return 0, &OpError{"pipe", c.net, c.raddr, err}, false
	}
	defer rPipe.Close()
	defer wPipe.Close()
	rPipeFd, wPipeFd := int(rPipe.Fd()), int(wPipe.Fd())
	pipeLen := 0
	readWait := readWaitFn(r)
	for remain > 0 || pipeLen > 0 {
		toRead := maxSendfileSize
		spliceFlags := 0
		if int64(toRead) >= remain {
			toRead = int(remain)
		} else {
			spliceFlags |= splice_f_more
		}
		// if we have stuff to read and we won't overflow pipeLen by reading more
		if pipeLen + toRead > pipeLen  {
			n, rerr := syscall.Splice(src, nil, wPipeFd, nil, toRead, spliceFlags)
			if n > 0 {
				remain -= n
				pipeLen += int(n)
			}
			if n == 0 && rerr == nil {
				break
			}
			rerr = handleSpliceErr(c, readWait, rerr)
			if rerr != nil {
				err = rerr
				break
			}
		}
		if pipeLen > 0 {
			n, werr := syscall.Splice(rPipeFd, nil, dst, nil, pipeLen, spliceFlags)
			if n > 0 {
				written += n
				pipeLen -= int(n)
			}
			if n == 0 && werr == nil {
				break
			}
			werr = handleSpliceErr(c, c.pd.WaitWrite, werr)
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

func fd(r io.Reader) (int, bool) {
	if f, ok := r.(*os.File); ok {
		return int(f.Fd()), true
	}
	if conn, ok := r.(*TCPConn); ok {
		return conn.fd.sysfd, true
	}
	return 0, false
}

func readWaitFn(r io.Reader) func() error {
	if conn, ok := r.(*TCPConn); ok {
		return conn.fd.pd.WaitRead;
	}
	return func() error { return nil }
}

func handleSpliceErr(c *netFD, waitFn func() error, err error) error {
	if err == syscall.EAGAIN {
		err = waitFn()
	}
	if err != nil {
		// This includes syscall.ENOSYS (no kernel
		// support) and syscall.EINVAL (fd types which
		// don't implement sendfile together)
		err = &OpError{"sendfile", c.net, c.raddr, err}
	}
	return err
}
