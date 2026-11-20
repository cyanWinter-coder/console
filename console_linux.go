//go:build linux
// +build linux

/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package console

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"golang.org/x/sys/unix"
)

const (
	maxEvents = 128
)

// Epoller manages multiple epoll consoles using edge-triggered epoll api so we
// dont have to deal with repeated wake-up of EPOLLER or EPOLLHUP.
// For more details, see:
// - https://github.com/systemd/systemd/pull/4262
// - https://github.com/moby/moby/issues/27202
//
// Example usage of Epoller and EpollConsole can be as follow:
//
//	epoller, _ := NewEpoller()
//	epollConsole, _ := epoller.Add(console)
//	go epoller.Wait()
//	var (
//		b  bytes.Buffer
//		wg sync.WaitGroup
//	)
//	wg.Add(1)
//	go func() {
//		io.Copy(&b, epollConsole)
//		wg.Done()
//	}()
//	// perform I/O on the console
//	epollConsole.Shutdown(epoller.CloseConsole)
//	wg.Wait()
//	epollConsole.Close()
type Epoller struct {
	efd       int
	mu        sync.Mutex
	fdMapping map[int]*EpollConsole
	closeOnce sync.Once
	// closed is guarded by mu and records whether Close has run. Add and
	// AddAll take mu so that a batch can never register against an epoll fd
	// that Close is tearing down concurrently.
	closed bool
}

// BatchError is returned by [Epoller.AddAll] when registering one of the
// consoles in a batch fails. The whole batch is rolled back before it is
// returned, so no console from the failed batch stays registered or visible
// through the epoller.
type BatchError struct {
	// Index is the position, within the slice passed to AddAll, of the
	// console whose registration failed.
	Index int
	// Err is the underlying error that caused the batch to fail.
	Err error
}

func (e *BatchError) Error() string {
	return fmt.Sprintf("failed to take over console at index %d: %v", e.Index, e.Err)
}

func (e *BatchError) Unwrap() error {
	return e.Err
}

// NewEpoller returns an instance of epoller with a valid epoll fd.
func NewEpoller() (*Epoller, error) {
	efd, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
	if err != nil {
		return nil, err
	}
	return &Epoller{
		efd:       efd,
		fdMapping: make(map[int]*EpollConsole),
	}, nil
}

// Add creates an epoll console based on the provided console. The console will
// be registered with EPOLLET (i.e. using edge-triggered notification) and its
// file descriptor will be set to non-blocking mode. After this, user should use
// the return console to perform I/O.
//
// It is a single-element wrapper around [Epoller.AddAll] and keeps the same
// behavior and error values as before.
func (e *Epoller) Add(console Console) (*EpollConsole, error) {
	consoles, err := e.AddAll([]Console{console})
	if err != nil {
		var berr *BatchError
		if errors.As(err, &berr) {
			// preserve the historical error surface of the single-console path
			return nil, berr.Err
		}
		return nil, err
	}
	return consoles[0], nil
}

// AddAll takes over several consoles as one atomic batch. Every console is
// switched to non-blocking mode and registered with EPOLLET, and none of them
// becomes visible to [Epoller.Wait], [Epoller.CloseConsole] or another AddAll
// until every registration and every index entry of the batch has succeeded.
//
// If any console fails, all consoles already registered as part of this batch
// are unregistered and their index entries removed before the call returns.
// The returned error is a [*BatchError] carrying the failing index and the
// underlying error. AddAll never closes a console it was handed: ownership of
// every console stays with the caller, including the consoles of a failed
// batch, which remain open in non-blocking mode.
func (e *Epoller) AddAll(consoles []Console) ([]*EpollConsole, error) {
	if len(consoles) == 0 {
		return []*EpollConsole{}, nil
	}

	epollConsoles := make([]*EpollConsole, len(consoles))
	// registered tracks the entries that this batch has published; on failure
	// it is exactly the set that must be undone.
	registered := make(map[int]struct{}, len(consoles))

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return nil, os.ErrClosed
	}

	for i, console := range consoles {
		epollConsole, err := e.addLocked(console)
		if err != nil {
			e.rollbackLocked(registered)
			return nil, &BatchError{Index: i, Err: err}
		}
		epollConsoles[i] = epollConsole
		registered[epollConsole.sysfd] = struct{}{}
	}
	return epollConsoles, nil
}

// addLocked registers one console and publishes its index entry. The caller
// must hold e.mu, which keeps the entry invisible to Wait and CloseConsole
// until the whole batch is committed.
func (e *Epoller) addLocked(console Console) (*EpollConsole, error) {
	sysfd := int(console.Fd())
	// Set sysfd to non-blocking mode
	if err := unix.SetNonblock(sysfd, true); err != nil {
		return nil, err
	}

	ev := unix.EpollEvent{
		Events: unix.EPOLLIN | unix.EPOLLOUT | unix.EPOLLRDHUP | unix.EPOLLET,
		Fd:     int32(sysfd),
	}
	if err := unix.EpollCtl(e.efd, unix.EPOLL_CTL_ADD, sysfd, &ev); err != nil {
		return nil, err
	}
	ef := &EpollConsole{
		Console: console,
		sysfd:   sysfd,
		readc:   sync.NewCond(&sync.Mutex{}),
		writec:  sync.NewCond(&sync.Mutex{}),
	}
	// Publishing only happens while mu is held, so a failed batch rolls back
	// before any concurrent reader can observe a half-initialized mapping.
	e.fdMapping[sysfd] = ef
	return ef, nil
}

// rollbackLocked undoes every registration published by the in-flight batch.
// The caller must hold e.mu. The epoll fd is still open and valid (Close is
// serialized on the same lock) and each listed fd was registered successfully
// moments ago, so EPOLL_CTL_DEL cannot fail in practice; the index entry is
// removed regardless so the mapping can never retain an orphan.
func (e *Epoller) rollbackLocked(registered map[int]struct{}) {
	for sysfd := range registered {
		delete(e.fdMapping, sysfd)
		_ = unix.EpollCtl(e.efd, unix.EPOLL_CTL_DEL, sysfd, &unix.EpollEvent{})
	}
}

// Wait starts the loop to wait for its consoles' notifications and signal
// appropriate console that it can perform I/O.
func (e *Epoller) Wait() error {
	events := make([]unix.EpollEvent, maxEvents)
	for {
		n, err := unix.EpollWait(e.efd, events, -1)
		if err != nil {
			// EINTR: The call was interrupted by a signal handler before either
			// any of the requested events occurred or the timeout expired
			if err == unix.EINTR {
				continue
			}
			return err
		}
		for i := 0; i < n; i++ {
			ev := &events[i]
			// the console is ready to be read from
			if ev.Events&(unix.EPOLLIN|unix.EPOLLHUP|unix.EPOLLERR) != 0 {
				if epfile := e.getConsole(int(ev.Fd)); epfile != nil {
					epfile.signalRead()
				}
			}
			// the console is ready to be written to
			if ev.Events&(unix.EPOLLOUT|unix.EPOLLHUP|unix.EPOLLERR) != 0 {
				if epfile := e.getConsole(int(ev.Fd)); epfile != nil {
					epfile.signalWrite()
				}
			}
		}
	}
}

// CloseConsole unregisters the console's file descriptor from epoll interface.
// It is serialized with [Epoller.AddAll] and [Epoller.Close] by e.mu, so it
// can only ever observe a fully committed batch, never a half-initialized one.
func (e *Epoller) CloseConsole(fd int) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.fdMapping, fd)
	if e.closed {
		// The epoll fd itself is gone; the index entry above was the only
		// remaining state to clean up.
		return nil
	}
	return unix.EpollCtl(e.efd, unix.EPOLL_CTL_DEL, fd, &unix.EpollEvent{})
}

func (e *Epoller) getConsole(sysfd int) *EpollConsole {
	e.mu.Lock()
	f := e.fdMapping[sysfd]
	e.mu.Unlock()
	return f
}

// Close closes the epoll fd. It takes the same lock as AddAll so that a batch
// in flight either finishes completely before the epoll fd is closed, or fails
// with [os.ErrClosed] without touching the closed fd.
func (e *Epoller) Close() error {
	closeErr := os.ErrClosed // default to "file already closed"
	e.closeOnce.Do(func() {
		e.mu.Lock()
		defer e.mu.Unlock()
		closeErr = unix.Close(e.efd)
		e.closed = true
	})
	return closeErr
}

// EpollConsole acts like a console but registers its file descriptor with an
// epoll fd and uses epoll API to perform I/O.
type EpollConsole struct {
	Console
	readc  *sync.Cond
	writec *sync.Cond
	sysfd  int
	closed bool
}

// Read reads up to len(p) bytes into p. It returns the number of bytes read
// (0 <= n <= len(p)) and any error encountered.
//
// If the console's read returns EAGAIN or EIO, we assume that it's a
// temporary error because the other side went away and wait for the signal
// generated by epoll event to continue.
func (ec *EpollConsole) Read(p []byte) (n int, err error) {
	var read int
	ec.readc.L.Lock()
	defer ec.readc.L.Unlock()
	for {
		read, err = ec.Console.Read(p[n:])
		n += read
		if err != nil {
			var hangup bool
			if perr, ok := err.(*os.PathError); ok {
				hangup = (perr.Err == unix.EAGAIN || perr.Err == unix.EIO)
			} else {
				hangup = (err == unix.EAGAIN || err == unix.EIO)
			}
			// if the other end disappear, assume this is temporary and wait for the
			// signal to continue again. Unless we didnt read anything and the
			// console is already marked as closed then we should exit
			if hangup && !(n == 0 && len(p) > 0 && ec.closed) {
				ec.readc.Wait()
				continue
			}
		}
		break
	}
	// if we didnt read anything then return io.EOF to end gracefully
	if n == 0 && len(p) > 0 && err == nil {
		err = io.EOF
	}
	// signal for others that we finished the read
	ec.readc.Signal()
	return n, err
}

// Writes len(p) bytes from p to the console. It returns the number of bytes
// written from p (0 <= n <= len(p)) and any error encountered that caused
// the write to stop early.
//
// If writes to the console returns EAGAIN or EIO, we assume that it's a
// temporary error because the other side went away and wait for the signal
// generated by epoll event to continue.
func (ec *EpollConsole) Write(p []byte) (n int, err error) {
	var written int
	ec.writec.L.Lock()
	defer ec.writec.L.Unlock()
	for {
		written, err = ec.Console.Write(p[n:])
		n += written
		if err != nil {
			var hangup bool
			if perr, ok := err.(*os.PathError); ok {
				hangup = (perr.Err == unix.EAGAIN || perr.Err == unix.EIO)
			} else {
				hangup = (err == unix.EAGAIN || err == unix.EIO)
			}
			// if the other end disappears, assume this is temporary and wait for the
			// signal to continue again.
			if hangup {
				ec.writec.Wait()
				continue
			}
		}
		// unrecoverable error, break the loop and return the error
		break
	}
	if n < len(p) && err == nil {
		err = io.ErrShortWrite
	}
	// signal for others that we finished the write
	ec.writec.Signal()
	return n, err
}

// Shutdown closes the file descriptor and signals call waiters for this fd.
// It accepts a callback which will be called with the console's fd. The
// callback typically will be used to do further cleanup such as unregister the
// console's fd from the epoll interface.
// User should call Shutdown and wait for all I/O operation to be finished
// before closing the console.
func (ec *EpollConsole) Shutdown(close func(int) error) error {
	ec.readc.L.Lock()
	defer ec.readc.L.Unlock()
	ec.writec.L.Lock()
	defer ec.writec.L.Unlock()

	ec.readc.Broadcast()
	ec.writec.Broadcast()
	ec.closed = true
	return close(ec.sysfd)
}

// signalRead signals that the console is readable.
func (ec *EpollConsole) signalRead() {
	ec.readc.L.Lock()
	ec.readc.Signal()
	ec.readc.L.Unlock()
}

// signalWrite signals that the console is writable.
func (ec *EpollConsole) signalWrite() {
	ec.writec.L.Lock()
	ec.writec.Signal()
	ec.writec.L.Unlock()
}
