package app

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestReloadRejectsUnregisteredProviderWithoutSwappingSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	writeProviderConfig(t, path, "openai")

	service, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if service.listener != nil {
			_ = service.listener.Close()
		}
		if err := service.Shutdown(context.Background()); err != nil {
			t.Errorf("Shutdown() error = %v", err)
		}
	})
	before := service.config.Load()

	writeProviderConfig(t, path, "acme")
	err = service.Reload()
	if err == nil || !strings.Contains(err.Error(), "unregistered provider") {
		t.Fatalf("Reload() error = %v, want unregistered provider", err)
	}
	if after := service.config.Load(); after != before {
		t.Fatal("Reload() swapped the live snapshot after provider validation failed")
	}
}

func TestShutdownDefersResourceCleanupUntilActiveHandlersExit(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	storeCloser := newSignalCloser()
	idleCloser := newSignalIdleCloser()
	service := &Service{
		listener:          listener,
		storeClose:        []io.Closer{storeCloser},
		upstreamTransport: idleCloser,
	}

	started := make(chan struct{})
	contextCanceled := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseHandler := func() { releaseOnce.Do(func() { close(release) }) }
	service.httpServer = &http.Server{Handler: service.trackRequests(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
		close(contextCanceled)
		<-release
	}))}
	t.Cleanup(func() {
		releaseHandler()
		_ = service.httpServer.Close()
		_ = listener.Close()
	})
	serveDone := make(chan error, 1)
	go func() { serveDone <- service.Serve() }()

	clientDone := make(chan error, 1)
	go func() {
		response, err := http.Get("http://" + listener.Addr().String())
		if response != nil {
			_ = response.Body.Close()
		}
		clientDone <- err
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		releaseHandler()
		t.Fatal("handler did not start")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	shutdownErr := service.Shutdown(ctx)
	if !errors.Is(shutdownErr, context.DeadlineExceeded) {
		releaseHandler()
		t.Fatalf("Shutdown() error = %v, want context deadline exceeded", shutdownErr)
	}
	select {
	case <-contextCanceled:
	case <-time.After(time.Second):
		releaseHandler()
		t.Fatal("forced server close did not cancel the request context")
	}
	if storeCloser.closed.Load() || idleCloser.closed.Load() {
		releaseHandler()
		t.Fatal("operational resources closed while the handler was still active")
	}

	releaseHandler()
	select {
	case <-storeCloser.done:
	case <-time.After(time.Second):
		t.Fatal("store was not closed after the handler exited")
	}
	select {
	case <-idleCloser.done:
	case <-time.After(time.Second):
		t.Fatal("upstream idle connections were not closed")
	}
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatalf("Serve() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve() did not stop")
	}
	select {
	case <-clientDone:
	case <-time.After(time.Second):
		t.Fatal("client request did not finish")
	}
}

type signalCloser struct {
	once   sync.Once
	closed atomic.Bool
	done   chan struct{}
}

func newSignalCloser() *signalCloser {
	return &signalCloser{done: make(chan struct{})}
}

func (c *signalCloser) Close() error {
	c.once.Do(func() {
		c.closed.Store(true)
		close(c.done)
	})
	return nil
}

type signalIdleCloser struct {
	once   sync.Once
	closed atomic.Bool
	done   chan struct{}
}

func newSignalIdleCloser() *signalIdleCloser {
	return &signalIdleCloser{done: make(chan struct{})}
}

func (c *signalIdleCloser) CloseIdleConnections() {
	c.once.Do(func() {
		c.closed.Store(true)
		close(c.done)
	})
}

func writeProviderConfig(t *testing.T, path, provider string) {
	t.Helper()
	content := `
server:
  listen: ":0"
auth:
  allow_anonymous: true
store:
  mode: memory
routes:
  demo:
    provider: ` + provider + `
    model: test-model
    base_url: http://example.invalid/v1
    capabilities:
      operations: [chat.completions]
      max_output_tokens: 4096
      tokenizer: cl100k_base
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}
