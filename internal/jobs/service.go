package jobs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Status string

const (
	StatusQueued     Status = "queued"
	StatusProcessing Status = "processing"
	StatusCompleted  Status = "completed"
	StatusFailed     Status = "failed"
)

// Errors returned by Service.Create. Callers should use errors.Is.
var (
	ErrQueueFull = errors.New("job queue is full")
	ErrStopped   = errors.New("service is stopping")
)

// errShutdownMessage is recorded on jobs that could not finish because the
// service was shut down before or while they were processed.
const errShutdownMessage = "service shut down before job completed"

type Job struct {
	ID        string    `json:"id"`
	Payload   string    `json:"payload"`
	Status    Status    `json:"status"`
	Error     string    `json:"error,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

type Processor interface {
	Process(ctx context.Context, payload string) error
}

type ProcessorFunc func(context.Context, string) error

func (f ProcessorFunc) Process(ctx context.Context, payload string) error {
	return f(ctx, payload)
}

type Service struct {
	// mu guards jobs, stopped and sends on queue. Holding it while sending
	// (non-blocking) and while closing queue guarantees we never send on a
	// closed channel.
	mu      sync.RWMutex
	jobs    map[string]*Job
	stopped bool
	queue   chan string

	processor Processor
	sequence  atomic.Uint64

	// ctx is passed to the processor; cancel aborts in-flight work.
	ctx    context.Context
	cancel context.CancelFunc

	wg       sync.WaitGroup
	stopOnce sync.Once
}

// NewService starts a fixed pool of workers that process jobs from a queue
// with the given capacity.
func NewService(workers, queueCapacity int) *Service {
	return newService(workers, queueCapacity, ProcessorFunc(defaultProcessor))
}

func newService(workers, queueCapacity int, p Processor) *Service {
	if workers < 1 {
		workers = 1
	}
	if queueCapacity < 1 {
		queueCapacity = 1
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Service{
		jobs:      make(map[string]*Job),
		queue:     make(chan string, queueCapacity),
		processor: p,
		ctx:       ctx,
		cancel:    cancel,
	}
	s.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go s.worker()
	}
	return s
}

func defaultProcessor(ctx context.Context, payload string) error {
	select {
	case <-time.After(100 * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}
	if payload == "" {
		return errors.New("empty payload")
	}
	if strings.Contains(payload, "fail") {
		return errors.New("simulated processing failure")
	}
	return nil
}

// Create registers a job and enqueues it. It never blocks: if the queue is
// full it returns ErrQueueFull, and after shutdown has begun it returns
// ErrStopped.
func (s *Service) Create(ctx context.Context, payload string) (*Job, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stopped {
		return nil, ErrStopped
	}

	id := fmt.Sprintf("job-%d", s.sequence.Add(1))
	job := &Job{
		ID:        id,
		Payload:   payload,
		Status:    StatusQueued,
		CreatedAt: time.Now().UTC(),
	}

	select {
	case s.queue <- id:
		// Worker cannot touch the job until we release mu, so registering
		// and cloning here is race-free.
		s.jobs[id] = job
		return cloneJob(job), nil
	default:
		return nil, ErrQueueFull
	}
}

func (s *Service) Get(id string) (*Job, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	job, ok := s.jobs[id]
	if !ok {
		return nil, false
	}
	return cloneJob(job), true
}

func (s *Service) worker() {
	defer s.wg.Done()
	for id := range s.queue {
		s.process(id)
	}
}

func (s *Service) process(id string) {
	s.mu.Lock()
	job, ok := s.jobs[id]
	if !ok {
		s.mu.Unlock()
		return
	}
	// Do not start new work once processing has been cancelled; just
	// drain the queue so every job ends in a terminal state.
	if s.ctx.Err() != nil {
		job.Status = StatusFailed
		job.Error = errShutdownMessage
		s.mu.Unlock()
		return
	}
	job.Status = StatusProcessing
	payload := job.Payload
	s.mu.Unlock()

	err := s.safeProcess(payload)

	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case err == nil:
		job.Status = StatusCompleted
	case s.ctx.Err() != nil && errors.Is(err, context.Canceled):
		job.Status = StatusFailed
		job.Error = errShutdownMessage
	default:
		job.Status = StatusFailed
		job.Error = err.Error()
	}
}

// safeProcess runs the processor and converts a panic into an error so a
// single bad job cannot kill a worker (and silently shrink the pool).
func (s *Service) safeProcess(payload string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("internal processing error")
		}
	}()
	return s.processor.Process(s.ctx, payload)
}

// Shutdown stops accepting new jobs and waits for queued and in-flight jobs
// to finish. If ctx is done first, in-flight processing is cancelled, the
// remaining queued jobs are marked failed, and ctx.Err() is returned once all
// workers have exited. Safe to call multiple times.
func (s *Service) Shutdown(ctx context.Context) error {
	s.stopOnce.Do(func() {
		s.mu.Lock()
		s.stopped = true
		close(s.queue)
		s.mu.Unlock()
	})

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		s.cancel()
		return nil
	case <-ctx.Done():
		s.cancel()
		<-done
		return ctx.Err()
	}
}

// Stop shuts the service down immediately: in-flight jobs are cancelled and
// queued jobs are marked failed. It returns once all workers have exited.
func (s *Service) Stop() {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = s.Shutdown(ctx)
}

func cloneJob(j *Job) *Job {
	c := *j
	return &c
}
