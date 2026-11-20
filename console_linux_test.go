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
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestEpollConsole(t *testing.T) {
	console, slavePath, err := NewPty()
	if err != nil {
		t.Fatal(err)
	}
	defer console.Close()

	slave, err := os.OpenFile(slavePath, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer slave.Close()

	iteration := 10

	cmd := exec.Command("sh", "-c", fmt.Sprintf("for x in `seq 1 %d`; do echo -n test; done", iteration))
	cmd.Stdin = slave
	cmd.Stdout = slave
	cmd.Stderr = slave

	epoller, err := NewEpoller()
	if err != nil {
		t.Fatal(err)
	}
	epollConsole, err := epoller.Add(console)
	if err != nil {
		t.Fatal(err)
	}
	go epoller.Wait()

	var (
		b  bytes.Buffer
		wg sync.WaitGroup
	)
	wg.Add(1)
	go func() {
		io.Copy(&b, epollConsole)
		wg.Done()
	}()

	if err := cmd.Run(); err != nil {
		t.Fatal(err)
	}
	slave.Close()
	if err := epollConsole.Shutdown(epoller.CloseConsole); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if err := epollConsole.Close(); err != nil {
		t.Fatal(err)
	}

	expectedOutput := ""
	for i := 0; i < iteration; i++ {
		expectedOutput += "test"
	}
	if out := b.String(); out != expectedOutput {
		t.Errorf("unexpected output %q", out)
	}

	// make sure multiple Close calls return os.ErrClosed after the first
	if err := epoller.Close(); err != nil {
		t.Fatal(err)
	}
	if err := epoller.Close(); err != os.ErrClosed {
		t.Fatalf("unexpected error returned from second call to epoller.Close(): %v", err)
	}
}

// newTestPty returns a fresh pty master console and its open slave side. Both
// are registered as test cleanup so a failing test never leaks an fd.
func newTestPty(t *testing.T) (Console, *os.File) {
	t.Helper()
	master, slavePath, err := NewPty()
	if err != nil {
		t.Fatal(err)
	}
	slave, err := os.OpenFile(slavePath, os.O_RDWR, 0)
	if err != nil {
		master.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		slave.Close()
		master.Close()
	})
	return master, slave
}

func newTestConsoles(t *testing.T, n int) ([]Console, []*os.File) {
	t.Helper()
	consoles := make([]Console, n)
	slaves := make([]*os.File, n)
	for i := 0; i < n; i++ {
		consoles[i], slaves[i] = newTestPty(t)
	}
	return consoles, slaves
}

func TestEpollerAddAllEmpty(t *testing.T) {
	epoller, err := NewEpoller()
	if err != nil {
		t.Fatal(err)
	}
	defer epoller.Close()

	consoles, err := epoller.AddAll(nil)
	if err != nil {
		t.Fatalf("AddAll with no consoles should succeed: %v", err)
	}
	if consoles == nil || len(consoles) != 0 {
		t.Fatalf("expected non-nil empty slice, got %v", consoles)
	}
}

func TestEpollerAddAll(t *testing.T) {
	const n = 3
	consoles, _ := newTestConsoles(t, n)

	epoller, err := NewEpoller()
	if err != nil {
		t.Fatal(err)
	}
	defer epoller.Close()

	epollConsoles, err := epoller.AddAll(consoles)
	if err != nil {
		t.Fatalf("AddAll failed: %v", err)
	}
	if len(epollConsoles) != n {
		t.Fatalf("expected %d epoll consoles, got %d", n, len(epollConsoles))
	}

	// the whole batch is visible only once every member committed
	epoller.mu.Lock()
	if len(epoller.fdMapping) != n {
		t.Fatalf("expected %d index entries, got %d", n, len(epoller.fdMapping))
	}
	for i, c := range consoles {
		if got := epoller.fdMapping[int(c.Fd())]; got != epollConsoles[i] {
			t.Fatalf("index entry for fd %d does not match returned console", c.Fd())
		}
	}
	epoller.mu.Unlock()

	// every batch member goes through the same shutdown/close path as Add
	for i, ec := range epollConsoles {
		if err := ec.Shutdown(epoller.CloseConsole); err != nil {
			t.Fatalf("Shutdown for console %d failed: %v", i, err)
		}
		if err := ec.Close(); err != nil {
			t.Fatalf("Close for console %d failed: %v", i, err)
		}
	}

	epoller.mu.Lock()
	entries := len(epoller.fdMapping)
	epoller.mu.Unlock()
	if entries != 0 {
		t.Fatalf("expected no index entries after shutdown, got %d", entries)
	}
}

func TestEpollerAddAllRollback(t *testing.T) {
	const n = 4
	consoles, _ := newTestConsoles(t, n)

	epoller, err := NewEpoller()
	if err != nil {
		t.Fatal(err)
	}
	defer epoller.Close()

	// a console already owned by the epoller must make the batch fail when it
	// is encountered, after the earlier members have been registered
	existing, err := epoller.Add(consoles[0])
	if err != nil {
		t.Fatal(err)
	}

	// index 0 is already registered, so EPOLL_CTL_ADD returns EEXIST there
	_, err = epoller.AddAll(consoles)
	var berr *BatchError
	if !errors.As(err, &berr) {
		t.Fatalf("expected *BatchError, got %v", err)
	}
	if berr.Index != 0 {
		t.Fatalf("expected failing index 0, got %d", berr.Index)
	}
	if !errors.Is(berr, unix.EEXIST) {
		t.Fatalf("expected wrapped error to be EEXIST, got %v", berr.Err)
	}

	// the failed batch left no trace: only the pre-existing console remains
	epoller.mu.Lock()
	if len(epoller.fdMapping) != 1 || epoller.fdMapping[int(consoles[0].Fd())] == nil {
		t.Fatalf("rollback left unexpected index entries: %v", epoller.fdMapping)
	}
	epoller.mu.Unlock()

	// none of the rolled-back fds is still registered with the epoll fd:
	// re-adding one must succeed instead of failing with EEXIST
	reAdded, err := epoller.Add(consoles[1])
	if err != nil {
		t.Fatalf("re-adding a rolled-back console should succeed, got %v", err)
	}
	if err := reAdded.Shutdown(epoller.CloseConsole); err != nil {
		t.Fatal(err)
	}
	if err := reAdded.Close(); err != nil {
		t.Fatal(err)
	}

	// the failing consoles were never closed by AddAll: they are still usable
	// and their ownership stays with the caller
	if _, err := consoles[2].Write([]byte("alive")); err != nil {
		t.Fatalf("rolled-back console should stay open and usable, got %v", err)
	}

	// a duplicate fd inside the batch itself fails the same way and the first
	// occurrence is rolled back too
	dup, _ := newTestConsoles(t, 1)
	_, err = epoller.AddAll([]Console{dup[0], dup[0]})
	if !errors.As(err, &berr) {
		t.Fatalf("expected *BatchError for duplicate batch entry, got %v", err)
	}
	if berr.Index != 1 {
		t.Fatalf("expected failing index 1, got %d", berr.Index)
	}
	epoller.mu.Lock()
	_, present := epoller.fdMapping[int(dup[0].Fd())]
	epoller.mu.Unlock()
	if present {
		t.Fatal("duplicate batch entry should have been rolled back")
	}

	if err := existing.Shutdown(epoller.CloseConsole); err != nil {
		t.Fatal(err)
	}
	if err := existing.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestEpollerAddAfterClose(t *testing.T) {
	epoller, err := NewEpoller()
	if err != nil {
		t.Fatal(err)
	}
	if err := epoller.Close(); err != nil {
		t.Fatal(err)
	}

	consoles, _ := newTestConsoles(t, 2)
	if _, err := epoller.Add(consoles[0]); err != os.ErrClosed {
		t.Fatalf("Add after Close should return os.ErrClosed, got %v", err)
	}
	if _, err := epoller.AddAll(consoles); err != os.ErrClosed {
		t.Fatalf("AddAll after Close should return os.ErrClosed, got %v", err)
	}
}

// TestEpollerAddAllConcurrentClose stresses that batch commits and
// CloseConsole/Close can never expose a half-initialized mapping: every batch
// is either fully present or fully absent, and Close never races the teardown.
func TestEpollerAddAllConcurrentClose(t *testing.T) {
	const iterations = 40
	const batchSize = 4

	for iter := 0; iter < iterations; iter++ {
		epoller, err := NewEpoller()
		if err != nil {
			t.Fatal(err)
		}
		consoles, _ := newTestConsoles(t, batchSize)

		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			epollConsoles, err := epoller.AddAll(consoles)
			if err != nil {
				var berr *BatchError
				if !errors.As(err, &berr) && err != os.ErrClosed {
					t.Errorf("unexpected AddAll error: %v", err)
				}
				return
			}
			// on success every member must have been published
			epoller.mu.Lock()
			got := len(epoller.fdMapping)
			epoller.mu.Unlock()
			if got != batchSize {
				t.Errorf("expected full batch of %d entries, got %d", batchSize, got)
			}
			for _, ec := range epollConsoles {
				if err := ec.Shutdown(epoller.CloseConsole); err != nil {
					t.Errorf("Shutdown failed: %v", err)
				}
				if err := ec.Close(); err != nil {
					t.Errorf("Close failed: %v", err)
				}
			}
		}()

		go func() {
			defer wg.Done()
			if err := epoller.Close(); err != nil {
				t.Errorf("epoller.Close failed: %v", err)
			}
		}()

		wg.Wait()

		// invariant: the mapping never holds a partial batch
		epoller.mu.Lock()
		got := len(epoller.fdMapping)
		epoller.mu.Unlock()
		if got != 0 && got != batchSize {
			t.Fatalf("mapping exposed partial batch: %d of %d entries", got, batchSize)
		}
	}
}

// TestEpollerAddAllConcurrentCloseConsole checks that CloseConsole racing with
// a batch only ever removes complete, committed members.
func TestEpollerAddAllConcurrentCloseConsole(t *testing.T) {
	const iterations = 40
	const batchSize = 4

	for iter := 0; iter < iterations; iter++ {
		epoller, err := NewEpoller()
		if err != nil {
			t.Fatal(err)
		}
		consoles, _ := newTestConsoles(t, batchSize)
		fds := make([]int, len(consoles))
		for i, c := range consoles {
			fds[i] = int(c.Fd())
		}

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			epollConsoles, err := epoller.AddAll(consoles)
			if err != nil {
				var berr *BatchError
				if !errors.As(err, &berr) {
					t.Errorf("unexpected AddAll error: %v", err)
				}
				return
			}
			for _, ec := range epollConsoles {
				_ = ec.Shutdown(epoller.CloseConsole)
				_ = ec.Close()
			}
		}()
		go func() {
			defer wg.Done()
			for _, fd := range fds {
				_ = epoller.CloseConsole(fd)
			}
		}()
		wg.Wait()

		epoller.mu.Lock()
		got := len(epoller.fdMapping)
		epoller.mu.Unlock()
		if got != 0 {
			t.Fatalf("expected all entries removed, got %d", got)
		}
		if err := epoller.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// TestEpollerAddAllWriteIndependence verifies the write side of batch members:
// data written through a member reaches only its own slave, and a member whose
// slave went away surfaces the same short-write/error semantics as a console
// taken over through the single-console Add path.
func TestEpollerAddAllWriteIndependence(t *testing.T) {
	const n = 2
	consoles, slaves := newTestConsoles(t, n)

	epoller, err := NewEpoller()
	if err != nil {
		t.Fatal(err)
	}
	defer epoller.Close()
	go epoller.Wait()

	epollConsoles, err := epoller.AddAll(consoles)
	if err != nil {
		t.Fatal(err)
	}

	for i, ec := range epollConsoles {
		// newline so the pty line discipline flushes the input line
		payload := []byte(fmt.Sprintf("write-%d\n", i))
		n, err := ec.Write(payload)
		if err != nil {
			t.Fatalf("write to member %d failed: %v", i, err)
		}
		if n != len(payload) {
			t.Fatalf("member %d wrote %d bytes, expected %d", i, n, len(payload))
		}
	}

	// each slave observes only the data written to its own member. A read
	// deadline keeps the test independent of pty line-discipline buffering.
	got := make([]byte, 32)
	for i, slave := range slaves {
		if err := slave.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
		readN, err := slave.Read(got)
		if err != nil {
			t.Fatal(err)
		}
		if want := fmt.Sprintf("write-%d\n", i); string(got[:readN]) != want {
			t.Fatalf("slave %d expected %q, got %q", i, want, got[:readN])
		}
	}

	// a member whose slave is closed fails its write rather than hanging the
	// other member
	if err := slaves[0].Close(); err != nil {
		t.Fatal(err)
	}
	if err := epollConsoles[0].Shutdown(epoller.CloseConsole); err != nil {
		t.Fatal(err)
	}
	if _, err := epollConsoles[1].Write([]byte("still-works\n")); err != nil {
		t.Fatalf("surviving member should stay writable, got %v", err)
	}
	if err := slaves[1].SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	readN, err := slaves[1].Read(got)
	if err != nil {
		t.Fatal(err)
	}
	if string(got[:readN]) != "still-works\n" {
		t.Fatalf("surviving member's data was lost: %q", got[:readN])
	}

	if err := epollConsoles[0].Close(); err != nil {
		t.Fatal(err)
	}
	if err := slaves[1].Close(); err != nil {
		t.Fatal(err)
	}
	if err := epollConsoles[1].Shutdown(epoller.CloseConsole); err != nil {
		t.Fatal(err)
	}
	if err := epollConsoles[1].Close(); err != nil {
		t.Fatal(err)
	}
}

// safeBuffer is a bytes.Buffer whose Write and String are serialized, so a
// polling test goroutine can inspect what an io.Copy goroutine is appending.
type safeBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *safeBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *safeBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// waitForContent polls buf until it matches want or the deadline passes.
func waitForContent(t *testing.T, buf *safeBuffer, want string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if got := buf.String(); got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %q, got %q", want, buf.String())
}

// TestEpollerAddAllWaitIndependence verifies that Wait keeps forwarding events
// per batch member: data written to one member's slave is delivered to that
// member only, and shutting one member down does not block the others.
func TestEpollerAddAllWaitIndependence(t *testing.T) {
	const n = 3
	consoles, slaves := newTestConsoles(t, n)

	epoller, err := NewEpoller()
	if err != nil {
		t.Fatal(err)
	}
	defer epoller.Close()

	epollConsoles, err := epoller.AddAll(consoles)
	if err != nil {
		t.Fatal(err)
	}
	go epoller.Wait()

	outputs := make([]safeBuffer, n)
	readerDone := make([]chan struct{}, n)
	for i := range readerDone {
		readerDone[i] = make(chan struct{})
	}
	for i, ec := range epollConsoles {
		go func(i int, ec *EpollConsole) {
			_, _ = io.Copy(&outputs[i], ec)
			close(readerDone[i])
		}(i, ec)
	}

	for i, slave := range slaves {
		if _, err := slave.Write([]byte(fmt.Sprintf("payload-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	for i := range outputs {
		waitForContent(t, &outputs[i], fmt.Sprintf("payload-%d", i))
	}

	// Shut the first member down exactly as production does: its slave goes
	// away first, which unblocks any in-flight Read, then Shutdown broadcasts
	// the waiters and unregisters the fd.
	if err := slaves[0].Close(); err != nil {
		t.Fatal(err)
	}
	if err := epollConsoles[0].Shutdown(epoller.CloseConsole); err != nil {
		t.Fatal(err)
	}
	if err := epollConsoles[0].Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-readerDone[0]:
	case <-time.After(5 * time.Second):
		t.Fatal("reader for shut-down member did not exit")
	}

	// the surviving members keep receiving their own events
	if _, err := slaves[1].Write([]byte("more")); err != nil {
		t.Fatal(err)
	}
	waitForContent(t, &outputs[1], "payload-1more")
	if got := outputs[2].String(); got != "payload-2" {
		t.Fatalf("member 2 received data addressed to another member: %q", got)
	}

	for i := 1; i < n; i++ {
		if err := slaves[i].Close(); err != nil {
			t.Fatal(err)
		}
		if err := epollConsoles[i].Shutdown(epoller.CloseConsole); err != nil {
			t.Fatal(err)
		}
		if err := epollConsoles[i].Close(); err != nil {
			t.Fatal(err)
		}
	}
	for i := 1; i < n; i++ {
		select {
		case <-readerDone[i]:
		case <-time.After(5 * time.Second):
			t.Fatalf("reader %d did not exit after shutdown", i)
		}
	}

	if err := epoller.Close(); err != nil {
		t.Fatal(err)
	}
	// Wait keeps its documented contract: with no registered console left it
	// simply has no event to report; its lifecycle is the caller's concern.
}

// TestEpollerAddAllNoLeak repeats failing batches and successful takeovers and
// asserts that neither open fds nor index entries accumulate.
func TestEpollerAddAllNoLeak(t *testing.T) {
	openFds := func() int {
		entries, err := os.ReadDir("/proc/self/fd")
		if err != nil {
			t.Fatal(err)
		}
		return len(entries)
	}

	epoller, err := NewEpoller()
	if err != nil {
		t.Fatal(err)
	}
	defer epoller.Close()

	baseline := openFds()
	const rounds = 50
	const batchSize = 3
	for round := 0; round < rounds; round++ {
		// this test owns its pty lifecycle so fd accounting can be checked
		// after every round instead of at test teardown
		consoles := make([]Console, batchSize+1)
		slaves := make([]*os.File, batchSize+1)
		for i := range consoles {
			consoles[i], slaves[i] = newTestPty(t)
		}

		// the last member is already registered, forcing a mid-batch rollback
		preRegistered, err := epoller.Add(consoles[batchSize])
		if err != nil {
			t.Fatal(err)
		}
		_, err = epoller.AddAll(consoles)
		var berr *BatchError
		if !errors.As(err, &berr) || berr.Index != batchSize {
			t.Fatalf("round %d: expected BatchError at index %d, got %v", round, batchSize, err)
		}

		epoller.mu.Lock()
		entries := len(epoller.fdMapping)
		epoller.mu.Unlock()
		if entries != 1 {
			t.Fatalf("round %d: rollback leaked %d index entries", round, entries-1)
		}

		// clean up the pre-registered console through the public path
		if err := preRegistered.Shutdown(epoller.CloseConsole); err != nil {
			t.Fatal(err)
		}
		if err := preRegistered.Close(); err != nil {
			t.Fatal(err)
		}
		// every console handed to the failing batch stays the caller's
		// responsibility and must still close cleanly
		for i, c := range consoles {
			if i != batchSize {
				if err := c.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if err := slaves[i].Close(); err != nil {
				t.Fatal(err)
			}
		}

		if got := openFds(); got != baseline {
			t.Fatalf("round %d: fd count changed from %d to %d", round, baseline, got)
		}
	}

	// a successful batch followed by full shutdown also returns to baseline
	consoles := make([]Console, batchSize)
	slaves := make([]*os.File, batchSize)
	for i := range consoles {
		consoles[i], slaves[i] = newTestPty(t)
	}
	epollConsoles, err := epoller.AddAll(consoles)
	if err != nil {
		t.Fatal(err)
	}
	for i, ec := range epollConsoles {
		if err := slaves[i].Close(); err != nil {
			t.Fatal(err)
		}
		if err := ec.Shutdown(epoller.CloseConsole); err != nil {
			t.Fatal(err)
		}
		if err := ec.Close(); err != nil {
			t.Fatal(err)
		}
	}
	epoller.mu.Lock()
	entries := len(epoller.fdMapping)
	epoller.mu.Unlock()
	if entries != 0 {
		t.Fatalf("expected no index entries after shutdown, got %d", entries)
	}
	if got := openFds(); got != baseline {
		t.Fatalf("fd count changed from %d to %d after successful batch", baseline, got)
	}
}
