package jobs

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// blockingProcessor blocks every job until release is closed (or ctx is
// cancelled) and records how many jobs run at once.
type blockingProcessor struct {
	release  chan struct{}
	started  chan string
	inFlight atomic.Int32
	maxSeen  atomic.Int32
}

func newBlockingProcessor() *blockingProcessor {
	return &blockingProcessor{
		release: make(chan struct{}),
		started: make(chan string, 100),
	}
}

func (p *blockingProcessor) Process(ctx context.Context, payload string) error {
	n := p.inFlight.Add(1)
	defer p.inFlight.Add(-1)
	for {
		m := p.maxSeen.Load()
		if n <= m || p.maxSeen.CompareAndSwap(m, n) {
			break
		}
	}
	p.started <- payload
	select {
	case <-p.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func waitForStatus(t *testing.T, s *Service, id string, want Status) *Job {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got, ok := s.Get(id)
		if !ok {
			t.Fatalf("job %s not found", id)
		}
		if got.Status == want {
			return got
		}
		time.Sleep(5 * time.Millisecond)
	}
	got, _ := s.Get(id)
	t.Fatalf("job %s: want status %s, got %s", id, want, got.Status)
	return nil
}

func waitStarted(t *testing.T, p *blockingProcessor, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case <-p.started:
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d of %d jobs started", i, n)
		}
	}
}

func TestCreateAndGet(t *testing.T) {
	s := NewService(1, 4)
	defer s.Stop()

	job, err := s.Create(context.Background(), "hello")
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != StatusQueued {
		t.Fatalf("expected queued, got %s", job.Status)
	}

	got, ok := s.Get(job.ID)
	if !ok || got.ID != job.ID {
		t.Fatalf("job not found")
	}
}

func TestGetUnknown(t *testing.T) {
	s := NewService(1, 1)
	defer s.Stop()
	if _, ok := s.Get("nope"); ok {
		t.Fatal("expected unknown job to be missing")
	}
}

func TestJobLifecycle(t *testing.T) {
	p := newBlockingProcessor()
	s := newService(1, 4, p)
	defer s.Stop()

	job, err := s.Create(context.Background(), "hello")
	if err != nil {
		t.Fatal(err)
	}
	waitStarted(t, p, 1)
	waitForStatus(t, s, job.ID, StatusProcessing)
	close(p.release)
	got := waitForStatus(t, s, job.ID, StatusCompleted)
	if got.Error != "" {
		t.Fatalf("unexpected error %q", got.Error)
	}
}

func TestFailedJob(t *testing.T) {
	s := NewService(1, 4)
	defer s.Stop()

	job, err := s.Create(context.Background(), "please fail")
	if err != nil {
		t.Fatal(err)
	}
	got := waitForStatus(t, s, job.ID, StatusFailed)
	if got.Error == "" {
		t.Fatal("expected failure message")
	}
}

func TestProcessorPanicFailsJobAndKeepsWorker(t *testing.T) {
	var calls atomic.Int32
	s := newService(1, 4, ProcessorFunc(func(ctx context.Context, payload string) error {
		if calls.Add(1) == 1 {
			panic("boom")
		}
		return nil
	}))
	defer s.Stop()

	bad, _ := s.Create(context.Background(), "a")
	good, _ := s.Create(context.Background(), "b")
	if got := waitForStatus(t, s, bad.ID, StatusFailed); got.Error == "" {
		t.Fatal("expected error message on panicked job")
	}
	waitForStatus(t, s, good.ID, StatusCompleted)
}

func TestConcurrentCreate(t *testing.T) {
	s := NewService(4, 100)
	defer s.Stop()

	const n = 50
	ids := make(chan string, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			job, err := s.Create(context.Background(), "hello")
			if err != nil {
				t.Errorf("create: %v", err)
				return
			}
			ids <- job.ID
			s.Get(job.ID) // concurrent reads alongside workers
		}()
	}
	wg.Wait()
	close(ids)

	seen := make(map[string]bool)
	for id := range ids {
		if seen[id] {
			t.Fatalf("duplicate id %s", id)
		}
		seen[id] = true
	}
	if len(seen) != n {
		t.Fatalf("want %d unique ids, got %d", n, len(seen))
	}
}

func TestWorkersRunConcurrentlyAndBounded(t *testing.T) {
	const workers = 3
	p := newBlockingProcessor()
	s := newService(workers, 10, p)
	defer s.Stop()

	for i := 0; i < 8; i++ {
		if _, err := s.Create(context.Background(), "x"); err != nil {
			t.Fatal(err)
		}
	}
	waitStarted(t, p, workers) // all workers busy at once
	select {
	case <-p.started:
		t.Fatal("more jobs started than there are workers")
	case <-time.After(50 * time.Millisecond):
	}
	if got := p.maxSeen.Load(); got != workers {
		t.Fatalf("max concurrent = %d, want %d", got, workers)
	}
	close(p.release)
}

func TestQueueFullFailsFastAndDoesNotLeakJob(t *testing.T) {
	p := newBlockingProcessor()
	s := newService(1, 1, p)
	defer s.Stop()

	if _, err := s.Create(context.Background(), "busy"); err != nil {
		t.Fatal(err)
	}
	waitStarted(t, p, 1) // worker occupied
	if _, err := s.Create(context.Background(), "queued"); err != nil {
		t.Fatal(err) // fills the single queue slot
	}

	done := make(chan error, 1)
	go func() {
		_, err := s.Create(context.Background(), "overflow")
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, ErrQueueFull) {
			t.Fatalf("want ErrQueueFull, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Create blocked on a full queue")
	}

	// The rejected job must not be visible.
	if _, ok := s.Get("job-3"); ok {
		t.Fatal("rejected job was stored")
	}
	close(p.release)
}

func TestCreateWithCancelledContext(t *testing.T) {
	s := NewService(1, 1)
	defer s.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Create(ctx, "x"); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
}

func TestCreateAfterStop(t *testing.T) {
	s := NewService(1, 1)
	s.Stop()
	s.Stop() // idempotent
	if _, err := s.Create(context.Background(), "x"); !errors.Is(err, ErrStopped) {
		t.Fatalf("want ErrStopped, got %v", err)
	}
}

// Create racing with Stop must never panic (send on closed channel).
func TestCreateConcurrentWithStop(t *testing.T) {
	for i := 0; i < 20; i++ {
		s := NewService(2, 50)
		var wg sync.WaitGroup
		for j := 0; j < 20; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := s.Create(context.Background(), "x")
				if err != nil && !errors.Is(err, ErrStopped) && !errors.Is(err, ErrQueueFull) {
					t.Errorf("unexpected error: %v", err)
				}
			}()
		}
		s.Stop()
		wg.Wait()
	}
}

func TestShutdownDrainsQueue(t *testing.T) {
	s := newService(2, 10, ProcessorFunc(func(ctx context.Context, _ string) error {
		time.Sleep(10 * time.Millisecond)
		return nil
	}))

	var ids []string
	for i := 0; i < 6; i++ {
		job, err := s.Create(context.Background(), "x")
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, job.ID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	for _, id := range ids {
		if got, _ := s.Get(id); got.Status != StatusCompleted {
			t.Fatalf("job %s: want completed, got %s", id, got.Status)
		}
	}
}

func TestShutdownTimeoutCancelsWork(t *testing.T) {
	p := newBlockingProcessor() // never released: only ctx can end it
	s := newService(1, 4, p)

	running, _ := s.Create(context.Background(), "running")
	waitStarted(t, p, 1)
	queued, _ := s.Create(context.Background(), "queued")

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- s.Shutdown(ctx) }()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("want DeadlineExceeded, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown did not return; workers still running")
	}

	for _, id := range []string{running.ID, queued.ID} {
		got, _ := s.Get(id)
		if got.Status != StatusFailed || got.Error != errShutdownMessage {
			t.Fatalf("job %s: got status=%s error=%q", id, got.Status, got.Error)
		}
	}
	if n := p.inFlight.Load(); n != 0 {
		t.Fatalf("%d processors still running", n)
	}
}
