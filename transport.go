// transport.go
//
// Copyright (c) 2018-2025 Junpei Kawamoto
//
// This software is released under the MIT License.
//
// http://opensource.org/licenses/mit-license.php

package pixeldrain

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/go-openapi/runtime"
)

// ErrIdleTimeout is returned when a download stalls due to idle timeout
var ErrIdleTimeout = errors.New("download stalled: no data received for timeout duration")

// ErrorUserCancelled is returned when a user cancels an operation
var ErrorUserCancelled = errors.New("operation cancelled by user")

// idleTimeoutBody wraps a response body to detect read timeouts
type idleTimeoutBody struct {
	body     io.ReadCloser
	timeout  time.Duration
	lastRead time.Time
	ctx      context.Context
	cancel   context.CancelCauseFunc
	mu       sync.Mutex
	done     chan struct{}
	closed   bool

	// Channels for read operations
	readReq  chan<- readRequest // Bidirectional channel for requests
	readResp <-chan readResult  // Read-only for receiving responses
}

type readRequest struct {
	buf []byte
}

type readResult struct {
	n   int
	err error
}

func newIdleTimeoutBody(body io.ReadCloser, timeout time.Duration, parentCtx context.Context) *idleTimeoutBody {
	ctx, cancel := context.WithCancelCause(parentCtx)

	// Create bidirectional channels
	reqCh := make(chan readRequest)
	respCh := make(chan readResult)

	itb := &idleTimeoutBody{
		body:     body,
		timeout:  timeout,
		ctx:      ctx,
		cancel:   cancel,
		lastRead: time.Now(),
		done:     make(chan struct{}),
		closed:   false,
		readReq:  reqCh,  // Bidirectional channel
		readResp: respCh, // Read-only channel
	}
	go itb.monitor()
	go itb.readWorker(reqCh, respCh)

	return itb
}

func (itb *idleTimeoutBody) readWorker(reqCh <-chan readRequest, respCh chan<- readResult) {
	defer close(respCh)

	for {
		select {
		case req, ok := <-reqCh:
			if !ok {
				// Channel closed, exit worker
				return
			}
			n, err := itb.body.Read(req.buf)
			select {
			case respCh <- readResult{n: n, err: err}:
				// Response sent successfully
			case <-itb.ctx.Done():
				// Context cancelled, stop worker
				return
			}
		case <-itb.ctx.Done():
			return
		}
	}
}

func (itb *idleTimeoutBody) Read(p []byte) (n int, err error) {
	// Check if context was cancelled first
	if err := context.Cause(itb.ctx); err != nil {
		return 0, err
	}

	// Send read request
	select {
	case itb.readReq <- readRequest{buf: p}:
		// Request sent successfully
	case <-itb.ctx.Done():
		return 0, context.Cause(itb.ctx)
	}

	// Wait for response
	select {
	case result, ok := <-itb.readResp:
		if !ok {
			// Channel closed due to context cancellation
			return 0, context.Cause(itb.ctx)
		}
		if result.n > 0 {
			itb.mu.Lock()
			itb.lastRead = time.Now()
			itb.mu.Unlock()
		}
		return result.n, result.err
	case <-itb.ctx.Done():
		return 0, context.Cause(itb.ctx)
	}
}

func (itb *idleTimeoutBody) Close() error {
	itb.mu.Lock()
	defer itb.mu.Unlock()

	if itb.closed {
		return nil
	}
	itb.closed = true

	// Cancel context and close channels
	itb.cancel(ErrorUserCancelled)
	close(itb.readReq)

	// Wait for goroutine to finish
	<-itb.done

	return itb.body.Close()
}

func (itb *idleTimeoutBody) monitor() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			itb.mu.Lock()
			if time.Since(itb.lastRead) > itb.timeout {
				itb.cancel(ErrIdleTimeout) // Cancel with specific cause
				itb.mu.Unlock()
				return
			}
			itb.mu.Unlock()
		case <-itb.done:
			// Request completed, stop monitoring
			return
		}
	}
}

// timeoutRoundTripper wraps responses with idle timeout detection
type timeoutRoundTripper struct {
	upstream http.RoundTripper
	timeout  time.Duration
}

// newTimeoutRoundTripper creates a RoundTripper that adds idle timeout to response bodies
func newTimeoutRoundTripper(upstream http.RoundTripper, timeout time.Duration) *timeoutRoundTripper {
	return &timeoutRoundTripper{
		upstream: upstream,
		timeout:  timeout,
	}
}

func (tr *timeoutRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := tr.upstream.RoundTrip(req)
	if err != nil {
		return nil, err
	}

	// Only wrap response body for successful responses that have a body
	if resp.StatusCode >= 200 && resp.StatusCode < 300 && resp.Body != nil {
		resp.Body = newIdleTimeoutBody(resp.Body, tr.timeout, req.Context())
	}

	return resp, nil
}

// roundTripper is a http.RoundTripper that forwards a request to the upstream and fixes content type header of the
// corresponding response.
type roundTripper struct {
	upstream    http.RoundTripper
	contentType string
}

var _ http.RoundTripper = (*roundTripper)(nil)

// newRoundTripper creates a roundTripper which wraps a given roundTripper and overwrites the content types of responses.
func newRoundTripper(upstream http.RoundTripper, contentType string) *roundTripper {
	return &roundTripper{
		upstream:    upstream,
		contentType: contentType,
	}
}

// RoundTrip executes a single HTTP transaction, returning a Response for the provided Request.
func (t *roundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	res, err := t.upstream.RoundTrip(req)
	if err != nil {
		return nil, err
	}

	if res.StatusCode < 300 {
		res.Header.Set(runtime.HeaderContentType, t.contentType)
	}
	return res, nil
}

// transport is a runtime.ClientTransport that modifies the http client of each request and forwards the request to the
// upstream transport.
type transport struct {
	upstream runtime.ClientTransport
}

var _ runtime.ClientTransport = (*transport)(nil)

// ContentTypeFixer returns a new ClientTransport that wraps the given ClientTransport to fix the content type issue.
func ContentTypeFixer(upstream runtime.ClientTransport) runtime.ClientTransport {
	return &transport{upstream: upstream}
}

// Submit sends the given operation and returns a response.
func (t *transport) Submit(op *runtime.ClientOperation) (interface{}, error) {
	if op.Client == nil {
		op.Client = &http.Client{
			Transport: http.DefaultTransport,
		}
	}

	// Add idle timeout for downloads (30 seconds)
	idleTimeout := 30 * time.Second
	timeoutTransport := newTimeoutRoundTripper(op.Client.Transport, idleTimeout)
	contentTypeTransport := newRoundTripper(timeoutTransport, op.ProducesMediaTypes[0])

	op.Client.Transport = contentTypeTransport
	return t.upstream.Submit(op)
}
