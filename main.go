package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/loggingingestion"
)

const (
	defaultBatchSize     = 100
	defaultFlushEvery    = 5 * time.Second
	defaultRetryBackoff  = 2 * time.Second
	defaultRetryQueueMax = 100 // max failed batches held in memory (~10 000 lines)
)

// logIngestionClient is the subset of loggingingestion.LoggingClient used by this program.
// Defined as an interface so tests can inject a mock without hitting the real API.
type logIngestionClient interface {
	PutLogs(ctx context.Context, request loggingingestion.PutLogsRequest) (loggingingestion.PutLogsResponse, error)
}

type config struct {
	logID           string
	source          string
	logType         string
	subject         string
	maxRetries      int
	ociConfigFile   string
	ociProfile      string
	pipePath        string
	healthPort      int
	healthThreshold time.Duration
}

// health tracks the last successful OCI send for the liveness probe.
type health struct {
	mu            sync.Mutex
	lastSuccessAt time.Time
	startedAt     time.Time
}

func (h *health) markSuccess() {
	h.mu.Lock()
	h.lastSuccessAt = time.Now()
	h.mu.Unlock()
}

// isHealthy returns true during the startup grace period, and afterward
// as long as a successful send occurred within the threshold window.
func (h *health) isHealthy(threshold time.Duration) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if time.Since(h.startedAt) < threshold {
		return true
	}
	return time.Since(h.lastSuccessAt) < threshold
}

func main() {
	cfg := parseConfig()

	provider, err := common.ConfigurationProviderFromFileWithProfile(expandHome(cfg.ociConfigFile), cfg.ociProfile, "")
	if err != nil {
		fmt.Fprintf(os.Stderr, "oci config: %v\n", err)
		os.Exit(1)
	}

	client, err := loggingingestion.NewLoggingClientWithConfigurationProvider(provider)
	if err != nil {
		fmt.Fprintf(os.Stderr, "oci client init: %v\n", err)
		os.Exit(1)
	}

	h := &health{startedAt: time.Now()}
	go runHealthServer(cfg.healthPort, cfg.healthThreshold, h)

	reader := setupInput(cfg.pipePath)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)

	b := &batcher{
		client:  client,
		cfg:     cfg,
		health:  h,
		buf:     make([]string, 0, defaultBatchSize),
		backoff: defaultRetryBackoff,
	}

	os.Exit(run(ctx, b, reader, sigs))
}

// readLines scans r line-by-line, sending each line to lines.
// It returns the first non-EOF read error, or nil on clean EOF.
func readLines(r io.Reader, lines chan<- string) error {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		lines <- scanner.Text()
	}
	return scanner.Err()
}

// run is the main event loop. It returns 0 on clean exit (EOF or signal) and
// 1 when the reader fails unexpectedly, so the caller can pass the code to
// os.Exit and let the process manager apply the appropriate restart policy.
func run(ctx context.Context, b *batcher, reader io.Reader, sigs <-chan os.Signal) int {
	lines := make(chan string, defaultBatchSize*2)
	readErrCh := make(chan error, 1)

	go func() {
		if err := readLines(reader, lines); err != nil {
			fmt.Fprintf(os.Stderr, "read error: %v\n", err)
			readErrCh <- err
		}
		close(lines)
	}()

	ticker := time.NewTicker(defaultFlushEvery)
	defer ticker.Stop()

	for {
		select {
		case line, ok := <-lines:
			if !ok {
				b.flush(ctx)
				select {
				case <-readErrCh:
					return 1
				default:
					return 0
				}
			}
			b.buf = append(b.buf, line)
			if len(b.buf) >= defaultBatchSize {
				b.flush(ctx)
			}
		case <-ticker.C:
			b.flush(ctx)
		case <-sigs:
			b.flush(ctx)
			return 0
		}
	}
}

func runHealthServer(port int, threshold time.Duration, h *health) {
	mux := http.NewServeMux()
	mux.Handle("/health", healthHandler(h, threshold))
	fmt.Fprintf(os.Stderr, "Health: http://localhost:%d/health\n", port)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", port), mux); err != nil {
		fmt.Fprintf(os.Stderr, "health server: %v\n", err)
	}
}

func healthHandler(h *health, threshold time.Duration) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !h.isHealthy(threshold) {
			http.Error(w, "stale: no successful send within threshold", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ok")
	}
}

func sendWithRetry(ctx context.Context, client logIngestionClient, cfg *config, lines []string, h *health, backoff time.Duration) error {
	entries := make([]loggingingestion.LogEntry, len(lines))
	now := common.SDKTime{Time: time.Now().UTC()}
	for i, l := range lines {
		id := uuid.New().String()
		line := l
		entries[i] = loggingingestion.LogEntry{
			Data: &line,
			Id:   &id,
			Time: &now,
		}
	}

	batch := loggingingestion.LogEntryBatch{
		Entries: entries,
		Source:  &cfg.source,
		Type:    &cfg.logType,
		Subject: &cfg.subject,
	}

	specVersion := "1.0"
	req := loggingingestion.PutLogsRequest{
		LogId: &cfg.logID,
		PutLogsDetails: loggingingestion.PutLogsDetails{
			Specversion:     &specVersion,
			LogEntryBatches: []loggingingestion.LogEntryBatch{batch},
		},
	}

	var lastErr error
	for attempt := 1; attempt <= cfg.maxRetries; attempt++ {
		_, err := client.PutLogs(ctx, req)
		if err == nil {
			h.markSuccess()
			fmt.Fprintf(os.Stderr, "sent %d lines\n", len(lines))
			return nil
		}
		lastErr = err
		fmt.Fprintf(os.Stderr, "attempt %d/%d failed: %v\n", attempt, cfg.maxRetries, err)
		if attempt < cfg.maxRetries {
			time.Sleep(backoff * time.Duration(attempt))
		}
	}
	return fmt.Errorf("all %d attempts failed: %w", cfg.maxRetries, lastErr)
}

// batcher accumulates log lines and flushes them to OCI in batches.
// Failed batches are held in retryQueue and re-sent before new data on the next flush.
type batcher struct {
	client     logIngestionClient
	cfg        *config
	health     *health
	buf        []string
	retryQueue [][]string
	backoff    time.Duration
}

func (b *batcher) flush(ctx context.Context) {
	// Drain retry queue before sending new lines.
	// Stop on first failure — OCI is likely still down, no point hammering it.
	drained := 0
	for _, batch := range b.retryQueue {
		if err := sendWithRetry(ctx, b.client, b.cfg, batch, b.health, b.backoff); err != nil {
			break
		}
		drained++
	}
	b.retryQueue = b.retryQueue[drained:]

	if len(b.buf) == 0 {
		return
	}
	if err := sendWithRetry(ctx, b.client, b.cfg, b.buf, b.health, b.backoff); err != nil {
		if len(b.retryQueue) >= defaultRetryQueueMax {
			fmt.Fprintf(os.Stderr, "retry queue full, evicting oldest batch (%d lines)\n", len(b.retryQueue[0]))
			b.retryQueue = b.retryQueue[1:]
		}
		saved := make([]string, len(b.buf))
		copy(saved, b.buf)
		b.retryQueue = append(b.retryQueue, saved)
	}
	b.buf = b.buf[:0]
}

// setupInput returns the reader to consume log lines from.
// Priority: fixed -pipe path > stdin pipe/redirect > PID-based daemon FIFO.
func setupInput(pipePath string) *os.File {
	if pipePath != "" {
		return openFIFO(pipePath, false)
	}
	if !isTerminal() {
		fmt.Fprintln(os.Stderr, "Reading from stdin...")
		return os.Stdin
	}
	return openFIFO(fmt.Sprintf("/tmp/oci-shipper-%d.pipe", os.Getpid()), true)
}

// openFIFO creates (or reuses) a named FIFO and returns it directly as the
// reader. It does not manipulate fd 0 / os.Stdin; the returned *os.File stays
// in Go's runtime poller so reads park the goroutine, not the OS thread.
// unlinkAfter removes the filesystem path after opening so the FIFO is only
// reachable via /proc/{pid}/fd/{n}.
func openFIFO(path string, unlinkAfter bool) *os.File {
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			fmt.Fprintf(os.Stderr, "mkdir %s: %v\n", dir, err)
			os.Exit(1)
		}
	}

	if err := syscall.Mkfifo(path, 0600); err != nil {
		if !os.IsExist(err) {
			fmt.Fprintf(os.Stderr, "mkfifo %s: %v\n", path, err)
			os.Exit(1)
		}
		fi, statErr := os.Stat(path)
		if statErr != nil || fi.Mode()&os.ModeNamedPipe == 0 {
			fmt.Fprintf(os.Stderr, "%s exists but is not a FIFO\n", path)
			os.Exit(1)
		}
	}

	f, err := os.OpenFile(path, os.O_RDWR, os.ModeNamedPipe)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open fifo %s: %v\n", path, err)
		os.Exit(1)
	}

	if unlinkAfter {
		os.Remove(path)
		fmt.Fprintf(os.Stderr, "PID: %d\n", os.Getpid())
		// f.Fd() prints the actual fd and intentionally sets blocking mode,
		// which is acceptable for the interactive (non-K8s) use case.
		fmt.Fprintf(os.Stderr, "Push logs with:\n  echo 'log line' > /proc/%d/fd/%d\n\n", os.Getpid(), f.Fd())
	} else {
		fmt.Fprintf(os.Stderr, "FIFO: %s\n", path)
		fmt.Fprintf(os.Stderr, "Push logs with:\n  echo 'log line' > %s\n\n", path)
	}

	return f
}

func parseConfig() *config {
	cfg := &config{}

	flag.StringVar(&cfg.logID, "log-id", envOr("OCI_LOG_ID", ""), "OCI Log OCID (env: OCI_LOG_ID)")
	flag.StringVar(&cfg.source, "source", envOr("OCI_LOG_SOURCE", "oci-shipper"), "log source (env: OCI_LOG_SOURCE)")
	flag.StringVar(&cfg.logType, "type", envOr("OCI_LOG_TYPE", "com.oraclecloud.logging.custom"), "log type (env: OCI_LOG_TYPE)")
	flag.StringVar(&cfg.subject, "subject", envOr("OCI_LOG_SUBJECT", ""), "log subject (env: OCI_LOG_SUBJECT)")
	flag.IntVar(&cfg.maxRetries, "max-retries", 3, "max send retries")
	flag.StringVar(&cfg.ociConfigFile, "oci-config", envOr("OCI_CONFIG_FILE", "~/.oci/config"), "OCI config file path (env: OCI_CONFIG_FILE)")
	flag.StringVar(&cfg.ociProfile, "oci-profile", envOr("OCI_CONFIG_PROFILE", "DEFAULT"), "OCI config profile (env: OCI_CONFIG_PROFILE)")
	flag.StringVar(&cfg.pipePath, "pipe", envOr("OCI_PIPE_PATH", ""), "fixed FIFO path for sidecar mode (env: OCI_PIPE_PATH)")
	flag.IntVar(&cfg.healthPort, "health-port", 8080, "HTTP health check port")
	flag.DurationVar(&cfg.healthThreshold, "health-threshold", 30*time.Second, "max time since last successful send before /health returns 503")
	flag.Parse()

	if cfg.logID == "" {
		fmt.Fprintln(os.Stderr, "error: -log-id or OCI_LOG_ID is required")
		flag.Usage()
		os.Exit(1)
	}

	return cfg
}

func expandHome(path string) string {
	if len(path) >= 2 && path[:2] == "~/" {
		if home, err := os.UserHomeDir(); err == nil {
			return home + path[1:]
		}
	}
	return path
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func isTerminal() bool {
	stat, err := os.Stdin.Stat()
	if err != nil {
		return false
	}
	return (stat.Mode() & os.ModeCharDevice) != 0
}
