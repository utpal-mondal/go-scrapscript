package jobs

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func newTestServer(t *testing.T, s *Service) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	RegisterHandlers(mux, s)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func do(t *testing.T, method, url, body string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q", ct)
	}
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return resp.StatusCode, out
}

func TestHTTPCreateAndGet(t *testing.T) {
	s := NewService(1, 4)
	defer s.Stop()
	srv := newTestServer(t, s)

	code, job := do(t, http.MethodPost, srv.URL+"/jobs", `{"payload":"hello"}`)
	if code != http.StatusCreated {
		t.Fatalf("status %d", code)
	}
	if job["status"] != string(StatusQueued) || job["id"] == "" || job["payload"] != "hello" {
		t.Fatalf("unexpected job %v", job)
	}

	id := job["id"].(string)
	deadline := time.Now().Add(2 * time.Second)
	for {
		code, got := do(t, http.MethodGet, srv.URL+"/jobs/"+id, "")
		if code != http.StatusOK {
			t.Fatalf("status %d", code)
		}
		if got["status"] == string(StatusCompleted) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("job not completed: %v", got)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestHTTPFailedJobExposesError(t *testing.T) {
	s := NewService(1, 4)
	defer s.Stop()
	srv := newTestServer(t, s)

	_, job := do(t, http.MethodPost, srv.URL+"/jobs", `{"payload":"this will fail"}`)
	id := job["id"].(string)
	deadline := time.Now().Add(2 * time.Second)
	for {
		_, got := do(t, http.MethodGet, srv.URL+"/jobs/"+id, "")
		if got["status"] == string(StatusFailed) {
			if got["error"] == nil || got["error"] == "" {
				t.Fatal("missing error message")
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("job not failed: %v", got)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestHTTPCreateValidation(t *testing.T) {
	s := NewService(1, 4)
	defer s.Stop()
	srv := newTestServer(t, s)

	tests := []struct {
		name string
		body string
		want int
	}{
		{"empty body", ``, http.StatusBadRequest},
		{"malformed", `{"payload":`, http.StatusBadRequest},
		{"wrong type", `{"payload":123}`, http.StatusBadRequest},
		{"missing payload", `{}`, http.StatusBadRequest},
		{"blank payload", `{"payload":"   "}`, http.StatusBadRequest},
		{"trailing data", `{"payload":"a"}{"payload":"b"}`, http.StatusBadRequest},
		{"too large", `{"payload":"` + strings.Repeat("a", maxRequestBody) + `"}`, http.StatusRequestEntityTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, body := do(t, http.MethodPost, srv.URL+"/jobs", tt.body)
			if code != tt.want {
				t.Fatalf("status %d, want %d (%v)", code, tt.want, body)
			}
			if body["error"] == nil {
				t.Fatal("missing error field")
			}
		})
	}
}

func TestHTTPGetNotFound(t *testing.T) {
	s := NewService(1, 4)
	defer s.Stop()
	srv := newTestServer(t, s)

	code, _ := do(t, http.MethodGet, srv.URL+"/jobs/does-not-exist", "")
	if code != http.StatusNotFound {
		t.Fatalf("status %d", code)
	}
}

func TestHTTPQueueFull(t *testing.T) {
	p := newBlockingProcessor()
	s := newService(1, 1, p)
	defer s.Stop()
	defer close(p.release)
	srv := newTestServer(t, s)

	do(t, http.MethodPost, srv.URL+"/jobs", `{"payload":"a"}`)
	waitStarted(t, p, 1)
	do(t, http.MethodPost, srv.URL+"/jobs", `{"payload":"b"}`)

	code, body := do(t, http.MethodPost, srv.URL+"/jobs", `{"payload":"c"}`)
	if code != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503 (%v)", code, body)
	}
}

func TestHTTPCreateAfterShutdown(t *testing.T) {
	s := NewService(1, 1)
	srv := newTestServer(t, s)
	_ = s.Shutdown(context.Background())

	code, _ := do(t, http.MethodPost, srv.URL+"/jobs", `{"payload":"a"}`)
	if code != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503", code)
	}
}
