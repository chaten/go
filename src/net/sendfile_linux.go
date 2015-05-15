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
const maxSendfileSize int = 4 << 20
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
	f, ok := r.(*os.File)
	if !ok {
		return 0, nil, false
	}

	if err := c.writeLock(); err != nil {
		return 0, err, true
	}
	defer c.writeUnlock()

	dst := c.sysfd
	src := int(f.Fd())
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
		if int64(toRead) > remain {
			toRead = int(remain)
		}
		// if we have stuff to read and we won't overflow pipeLen by reading more
		if pipeLen + toRead > pipeLen {
			n, rerr := syscall.Splice(src, nil, wPipeFd, nil, toRead, splice_f_more)
			if n > 0 {
				remain -= n
				pipeLen += int(n)
			}
			if n == 0 && rerr == nil {
				break
			}
			rerr = handleSpliceErr(c, rerr)
			if rerr != nil {
				err = rerr
				break
			}
		}
		if pipeLen > 0 {
			n, werr := syscall.Splice(rPipeFd, nil, dst, nil, pipeLen, splice_f_more)
			if n > 0 {
				written += n
				pipeLen -= int(n)
			}
			if n == 0 && werr == nil {
				break
			}
			werr = handleSpliceErr(c, werr)
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

func handleSpliceErr(c *netFD, err error) error {
	if err == syscall.EAGAIN {
		err = c.pd.WaitWrite()
	}
	if err != nil {
		// This includes syscall.ENOSYS (no kernel
		// support) and syscall.EINVAL (fd types which
		// don't implement sendfile together)
		err = &OpError{"sendfile", c.net, c.raddr, err}
	}
	return err
}
