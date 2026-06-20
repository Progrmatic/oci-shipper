package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/loggingingestion"
)

// mockClient records every PutLogs call and returns pre-configured responses.
// When responses are exhausted: returns nil (success) unless alwaysFail is set.
type mockClient struct {
	mu         sync.Mutex
	callCount  int
	responses  []error // returned in order; nil = success
	alwaysFail bool    // return error unconditionally once responses are exhausted
}

func (m *mockClient) PutLogs(_ context.Context, _ loggingingestion.PutLogsRequest) (loggingingestion.PutLogsResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	i := m.callCount
	m.callCount++
	if i < len(m.responses) {
		return loggingingestion.PutLogsResponse{}, m.responses[i]
	}
	if m.alwaysFail {
		return loggingingestion.PutLogsResponse{}, errors.New("oci down")
	}
	return loggingingestion.PutLogsResponse{}, nil
}

func (m *mockClient) calls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.callCount
}

func testCfg() *config {
	return &config{
		logID:      "ocid1.log.test",
		source:     "test-source",
		logType:    "com.test",
		subject:    "test-subject",
		maxRetries: 3,
	}
}

// ── sendWithRetry ────────────────────────────────────────────────────────────

func TestSendWithRetry_ImmediateSuccess(t *testing.T) {
	mc := &mockClient{responses: []error{nil}}
	h := &health{startedAt: time.Now()}

	err := sendWithRetry(context.Background(), mc, testCfg(), []string{"line 1"}, h, 0)

	if err != nil {
		t.Fatalf("expected success, got: %v", err)
	}
	if mc.calls() != 1 {
		t.Fatalf("expected 1 API call, got %d", mc.calls())
	}
}

func TestSendWithRetry_RetryThenSuccess(t *testing.T) {
	mc := &mockClient{responses: []error{
		errors.New("server error"),
		errors.New("server error"),
		nil, // succeeds on 3rd attempt
	}}
	h := &health{startedAt: time.Now()}

	err := sendWithRetry(context.Background(), mc, testCfg(), []string{"line 1"}, h, 0)

	if err != nil {
		t.Fatalf("expected eventual success, got: %v", err)
	}
	if mc.calls() != 3 {
		t.Fatalf("expected 3 API calls, got %d", mc.calls())
	}
}

func TestSendWithRetry_AllRetriesExhausted(t *testing.T) {
	mc := &mockClient{responses: []error{
		errors.New("fail 1"),
		errors.New("fail 2"),
		errors.New("fail 3"),
	}}
	h := &health{startedAt: time.Now()}

	err := sendWithRetry(context.Background(), mc, testCfg(), []string{"line 1"}, h, 0)

	if err == nil {
		t.Fatal("expected error after exhausted retries, got nil")
	}
	if mc.calls() != 3 {
		t.Fatalf("expected exactly 3 API calls, got %d", mc.calls())
	}
}

func TestSendWithRetry_RespectsMaxRetries(t *testing.T) {
	mc := &mockClient{responses: []error{
		errors.New("fail"), errors.New("fail"), errors.New("fail"),
		errors.New("fail"), errors.New("fail"),
	}}
	cfg := testCfg()
	cfg.maxRetries = 2
	h := &health{startedAt: time.Now()}

	sendWithRetry(context.Background(), mc, cfg, []string{"line"}, h, 0)

	if mc.calls() != 2 {
		t.Fatalf("expected exactly 2 API calls for maxRetries=2, got %d", mc.calls())
	}
}

func TestSendWithRetry_MarksHealthOnSuccess(t *testing.T) {
	mc := &mockClient{responses: []error{nil}}
	// Start outside the grace period so isHealthy depends solely on lastSuccessAt.
	h := &health{startedAt: time.Now().Add(-time.Hour)}

	if h.isHealthy(30 * time.Second) {
		t.Fatal("should be unhealthy before any successful send")
	}

	sendWithRetry(context.Background(), mc, testCfg(), []string{"line"}, h, 0)

	if !h.isHealthy(30 * time.Second) {
		t.Fatal("should be healthy after a successful send")
	}
}

func TestSendWithRetry_DoesNotMarkHealthOnAllFailures(t *testing.T) {
	mc := &mockClient{responses: []error{
		errors.New("fail"), errors.New("fail"), errors.New("fail"),
	}}
	h := &health{startedAt: time.Now().Add(-time.Hour)}

	sendWithRetry(context.Background(), mc, testCfg(), []string{"line"}, h, 0)

	if h.isHealthy(30 * time.Second) {
		t.Fatal("should remain unhealthy after all retries failed")
	}
}

func TestSendWithRetry_ExponentialBackoffCalled(t *testing.T) {
	mc := &mockClient{responses: []error{
		errors.New("fail"),
		errors.New("fail"),
		nil,
	}}
	h := &health{startedAt: time.Now()}
	backoff := 10 * time.Millisecond

	start := time.Now()
	sendWithRetry(context.Background(), mc, testCfg(), []string{"line"}, h, backoff)
	elapsed := time.Since(start)

	// backoff*1 + backoff*2 = 30ms minimum
	if elapsed < 30*time.Millisecond {
		t.Fatalf("backoff not applied: elapsed %v, want >= 30ms", elapsed)
	}
}

// ── health struct ────────────────────────────────────────────────────────────

func TestHealth_HealthyDuringGracePeriod(t *testing.T) {
	h := &health{startedAt: time.Now()}

	if !h.isHealthy(30 * time.Second) {
		t.Fatal("should be healthy within the startup grace period")
	}
}

func TestHealth_UnhealthyAfterGracePeriodWithNoSuccess(t *testing.T) {
	h := &health{startedAt: time.Now().Add(-time.Hour)}
	// lastSuccessAt is zero value (long in the past)

	if h.isHealthy(30 * time.Second) {
		t.Fatal("should be unhealthy past grace period with no successful send")
	}
}

func TestHealth_HealthyAfterRecentSuccess(t *testing.T) {
	h := &health{startedAt: time.Now().Add(-time.Hour)}
	h.markSuccess()

	if !h.isHealthy(30 * time.Second) {
		t.Fatal("should be healthy right after markSuccess")
	}
}

func TestHealth_UnhealthyWhenSuccessIsStale(t *testing.T) {
	h := &health{startedAt: time.Now().Add(-time.Hour)}
	h.mu.Lock()
	h.lastSuccessAt = time.Now().Add(-31 * time.Second)
	h.mu.Unlock()

	if h.isHealthy(30 * time.Second) {
		t.Fatal("should be unhealthy when last success is older than threshold")
	}
}

func TestHealth_RecoveryAfterStale(t *testing.T) {
	h := &health{startedAt: time.Now().Add(-time.Hour)}
	h.mu.Lock()
	h.lastSuccessAt = time.Now().Add(-time.Hour) // stale
	h.mu.Unlock()

	if h.isHealthy(30 * time.Second) {
		t.Fatal("should be unhealthy initially")
	}

	h.markSuccess()

	if !h.isHealthy(30 * time.Second) {
		t.Fatal("should recover after markSuccess")
	}
}

// ── batcher retry queue ──────────────────────────────────────────────────────

// newBatcher creates a batcher with zero backoff so retry loops finish instantly in tests.
func newBatcher(mc *mockClient) *batcher {
	return &batcher{
		client:  mc,
		cfg:     testCfg(),
		health:  &health{startedAt: time.Now()},
		buf:     make([]string, 0, defaultBatchSize),
		backoff: 0,
	}
}

// failN returns a slice of n identical errors, enough to exhaust all retries.
func failN(n int) []error {
	errs := make([]error, n)
	for i := range errs {
		errs[i] = errors.New("oci down")
	}
	return errs
}

func TestBatcher_FailedBatchEnqueued(t *testing.T) {
	// All 3 retry attempts fail → batch should be saved to retry queue.
	mc := &mockClient{responses: failN(3)}
	b := newBatcher(mc)
	b.buf = []string{"line 1", "line 2"}

	b.flush(context.Background())

	if len(b.retryQueue) != 1 {
		t.Fatalf("expected 1 batch in retry queue, got %d", len(b.retryQueue))
	}
	if len(b.retryQueue[0]) != 2 {
		t.Fatalf("expected queued batch to have 2 lines, got %d", len(b.retryQueue[0]))
	}
}

func TestBatcher_RetryQueueDrainedOnRecovery(t *testing.T) {
	// First flush: all 3 retries fail → batch queued.
	mc := &mockClient{responses: failN(3)}
	b := newBatcher(mc)
	b.buf = []string{"old line"}
	b.flush(context.Background())

	if len(b.retryQueue) != 1 {
		t.Fatalf("setup: expected 1 queued batch, got %d", len(b.retryQueue))
	}

	// Second flush: OCI recovered.
	// flush drains queue first (1 call), then sends new buf (1 call).
	mc.mu.Lock()
	mc.responses = []error{nil, nil}
	mc.callCount = 0
	mc.mu.Unlock()

	b.buf = []string{"new line"}
	b.flush(context.Background())

	if len(b.retryQueue) != 0 {
		t.Fatalf("expected retry queue empty after recovery, got %d batches", len(b.retryQueue))
	}
	if mc.calls() != 2 {
		t.Fatalf("expected 2 API calls (drain + new buf), got %d", mc.calls())
	}
}

func TestBatcher_RetryQueueEvictsOldestWhenFull(t *testing.T) {
	// alwaysFail ensures every API call fails regardless of how many times flush is called.
	mc := &mockClient{alwaysFail: true}
	b := newBatcher(mc)

	for i := 0; i < defaultRetryQueueMax+1; i++ {
		b.buf = []string{fmt.Sprintf("line %d", i)}
		b.flush(context.Background())
	}

	if len(b.retryQueue) != defaultRetryQueueMax {
		t.Fatalf("expected retry queue capped at %d, got %d", defaultRetryQueueMax, len(b.retryQueue))
	}
	// "line 0" (oldest) should have been evicted; first remaining is "line 1".
	if b.retryQueue[0][0] != "line 1" {
		t.Fatalf("expected oldest batch evicted, first remaining is %q", b.retryQueue[0][0])
	}
}

// ── health HTTP handler ──────────────────────────────────────────────────────

func TestHealthHandler_Returns200WhenHealthy(t *testing.T) {
	h := &health{startedAt: time.Now()} // within grace period
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	healthHandler(h, 30*time.Second).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if body := rec.Body.String(); body != "ok\n" {
		t.Fatalf("unexpected body: %q", body)
	}
}

func TestHealthHandler_Returns503WhenStale(t *testing.T) {
	h := &health{startedAt: time.Now().Add(-time.Hour)}
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	healthHandler(h, 30*time.Second).ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
}

func TestHealthHandler_Returns200AfterMarkSuccess(t *testing.T) {
	h := &health{startedAt: time.Now().Add(-time.Hour)}
	h.markSuccess()

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	healthHandler(h, 30*time.Second).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 after markSuccess, got %d", rec.Code)
	}
}

// ── readLines ────────────────────────────────────────────────────────────────

// failReader returns the given error on every Read call.
type failReader struct{ err error }

func (f *failReader) Read([]byte) (int, error) { return 0, f.err }

func TestReadLines_CleanEOF(t *testing.T) {
	r := strings.NewReader("alpha\nbeta\n")
	lines := make(chan string, 10)

	err := readLines(r, lines)
	close(lines)

	if err != nil {
		t.Fatalf("expected nil on clean EOF, got: %v", err)
	}
	var got []string
	for l := range lines {
		got = append(got, l)
	}
	if len(got) != 2 || got[0] != "alpha" || got[1] != "beta" {
		t.Fatalf("unexpected lines: %v", got)
	}
}

func TestReadLines_PropagatesReadError(t *testing.T) {
	want := errors.New("bad file descriptor")
	lines := make(chan string, 10)

	err := readLines(&failReader{err: want}, lines)

	if err == nil {
		t.Fatal("expected error from failing reader, got nil")
	}
	if err.Error() != want.Error() {
		t.Fatalf("expected %q, got %q", want, err)
	}
}

// ── run (event loop exit codes) ──────────────────────────────────────────────

func TestRun_ExitsZeroOnCleanEOF(t *testing.T) {
	r := strings.NewReader("line1\nline2\n")
	b := newBatcher(&mockClient{responses: []error{nil}})
	sigs := make(chan os.Signal)

	code := run(context.Background(), b, r, sigs)
	if code != 0 {
		t.Fatalf("expected exit code 0 on clean EOF, got %d", code)
	}
}

func TestRun_ExitsOneOnReadError(t *testing.T) {
	r := &failReader{err: errors.New("bad file descriptor")}
	b := newBatcher(&mockClient{})
	sigs := make(chan os.Signal)

	code := run(context.Background(), b, r, sigs)
	if code != 1 {
		t.Fatalf("expected exit code 1 on read error, got %d", code)
	}
}

func TestRun_ExitsZeroOnSignal(t *testing.T) {
	pr, pw := io.Pipe()
	defer pw.Close()
	b := newBatcher(&mockClient{})
	sigs := make(chan os.Signal, 1)

	go func() {
		time.Sleep(10 * time.Millisecond)
		sigs <- syscall.SIGTERM
	}()

	code := run(context.Background(), b, pr, sigs)
	if code != 0 {
		t.Fatalf("expected exit code 0 on SIGTERM, got %d", code)
	}
}

// ── openFIFO ─────────────────────────────────────────────────────────────────

func TestOpenFIFO_CreatesNewFIFO(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.pipe")

	f := openFIFO(path, false)
	if f == nil {
		t.Fatal("expected non-nil file")
	}
	f.Close()

	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat after openFIFO: %v", err)
	}
	if fi.Mode()&os.ModeNamedPipe == 0 {
		t.Fatal("expected named pipe at path")
	}
}

func TestOpenFIFO_ReusesExistingFIFO(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "existing.pipe")

	if err := syscall.Mkfifo(path, 0600); err != nil {
		t.Fatalf("setup mkfifo: %v", err)
	}

	// Should reuse the existing FIFO without error.
	f := openFIFO(path, false)
	if f == nil {
		t.Fatal("expected non-nil file on reuse of existing FIFO")
	}
	f.Close()
}
