package jobs

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
)

// maxRequestBody bounds the size of a POST /jobs body.
const maxRequestBody = 1 << 20 // 1 MiB

type createJobRequest struct {
	Payload string `json:"payload"`
}

func RegisterHandlers(mux *http.ServeMux, service *Service) {
	mux.HandleFunc("POST /jobs", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxRequestBody)
		dec := json.NewDecoder(r.Body)

		var req createJobRequest
		if err := dec.Decode(&req); err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
				return
			}
			writeError(w, http.StatusBadRequest, "invalid JSON")
			return
		}
		// Reject trailing data such as `{"payload":"a"}{"payload":"b"}`.
		if _, err := dec.Token(); !errors.Is(err, io.EOF) {
			writeError(w, http.StatusBadRequest, "invalid JSON")
			return
		}
		if strings.TrimSpace(req.Payload) == "" {
			writeError(w, http.StatusBadRequest, "payload is required")
			return
		}

		job, err := service.Create(r.Context(), req.Payload)
		switch {
		case err == nil:
			writeJSON(w, http.StatusCreated, job)
		case errors.Is(err, ErrQueueFull):
			w.Header().Set("Retry-After", "1")
			writeError(w, http.StatusServiceUnavailable, "job queue is full, retry later")
		case errors.Is(err, ErrStopped):
			writeError(w, http.StatusServiceUnavailable, "service is shutting down")
		default:
			// Client went away or something unexpected: don't leak details.
			log.Printf("create job: %v", err)
			writeError(w, http.StatusInternalServerError, "internal server error")
		}
	})

	mux.HandleFunc("GET /jobs/{id}", func(w http.ResponseWriter, r *http.Request) {
		job, ok := service.Get(r.PathValue("id"))
		if !ok {
			writeError(w, http.StatusNotFound, "job not found")
			return
		}
		writeJSON(w, http.StatusOK, job)
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
