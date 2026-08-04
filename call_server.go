package redis_ipc

// call_server.go — multi-method RPC server.
//
// CallServer is the higher-level companion to HandleCalls: instead of one
// request channel per RPC method (HandleCalls' shape), a single CallServer
// owns a per-service request channel and dispatches incoming envelopes by
// method name: one Redis key, one set of handlers. The reply leg is
// unchanged, the server still PUBLISHes to the per-call reply channel
// from the envelope. The request channel itself is served by the
// client's shared BRPOP loop (see mux.go).
//
// Wire compatibility: the request envelope adds an optional `method`
// field. Old Call/HandleCalls clients (no method) still work for their
// channel-per-RPC use case. CallMethod populates the field; CallServer
// dispatches on it.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrUnknownMethod is returned (server-side) when a request arrives for a
// method that has no registered handler. The caller receives a *CallError
// whose Msg is "unknown method: X".
var ErrUnknownMethod = errors.New("unknown method")

// CallMethod sends a typed request to a CallServer's per-service channel,
// addressed to a specific method. Same semantics as Call (sub-second
// timeouts, ErrCallTimeout / *CallError discrimination), just with an
// extra method field on the wire so a single channel can host many RPCs.
func CallMethod[Req, Resp any](c *Client, channel, method string, req Req, timeout time.Duration) (Resp, error) {
	var zero Resp
	if method == "" {
		return zero, fmt.Errorf("CallMethod: empty method")
	}

	payload, err := c.opts.codec.Encode(req)
	if err != nil {
		return zero, fmt.Errorf("encode request: %w", err)
	}

	id := genCallID()
	replyChannel := channel + ":reply:" + id
	env := callEnvelope{
		ID:           id,
		Method:       method,
		ReplyChannel: replyChannel,
		Deadline:     time.Now().Add(timeout).UnixMilli(),
		Payload:      payload,
	}
	envBytes, err := json.Marshal(env)
	if err != nil {
		return zero, fmt.Errorf("encode envelope: %w", err)
	}

	ctx, cancel := context.WithTimeout(c.ctx, timeout)
	defer cancel()

	pubsub := c.redis.Subscribe(ctx, replyChannel)
	defer pubsub.Close()

	if _, err := pubsub.Receive(ctx); err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return zero, ErrCallTimeout
		}
		return zero, fmt.Errorf("subscribe to reply channel: %w", err)
	}

	if err := c.redis.LPush(ctx, channel, envBytes).Err(); err != nil {
		return zero, fmt.Errorf("LPUSH request: %w", err)
	}

	msg, err := pubsub.ReceiveMessage(ctx)
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return zero, ErrCallTimeout
		}
		return zero, fmt.Errorf("ReceiveMessage: %w", err)
	}

	var reply callReply
	if err := json.Unmarshal([]byte(msg.Payload), &reply); err != nil {
		return zero, fmt.Errorf("decode reply: %w", err)
	}
	if !reply.OK {
		return zero, &CallError{Msg: reply.Error}
	}

	var resp Resp
	if err := c.opts.codec.Decode(reply.Payload, &resp); err != nil {
		return zero, fmt.Errorf("decode response: %w", err)
	}
	return resp, nil
}

// methodHandler is the type-erased server-side handler. It owns the
// generic decode + encode so RegisterCall callers can keep their own
// typed signature.
type methodHandler func(ctx context.Context, payload json.RawMessage) (json.RawMessage, error)

// CallServer is the multi-method RPC server. One BRPOP loop, one
// concurrency-bounded dispatcher, many registered handlers.
type CallServer struct {
	client  *Client
	channel string
	sem     chan struct{}
	sub     *queueSub

	mu       sync.RWMutex
	handlers map[string]methodHandler

	stopped  bool
	stopMu   sync.Mutex
	inflight sync.WaitGroup

	started bool
}

// CallServerOption configures a CallServer.
type CallServerOption func(*callServerOpts)

type callServerOpts struct {
	concurrency int
}

// WithCallServerConcurrency caps in-flight handler invocations across all
// methods on this server. Default 4.
func WithCallServerConcurrency(n int) CallServerOption {
	return func(o *callServerOpts) {
		if n > 0 {
			o.concurrency = n
		}
	}
}

// NewCallServer returns a CallServer bound to a per-service request
// channel (e.g. "motion:rpc"). Use RegisterCall to attach typed handlers,
// then Start to begin processing requests.
func NewCallServer(c *Client, channel string, opts ...CallServerOption) *CallServer {
	o := &callServerOpts{concurrency: 4}
	for _, opt := range opts {
		opt(o)
	}
	return &CallServer{
		client:   c,
		channel:  channel,
		sem:      make(chan struct{}, o.concurrency),
		handlers: make(map[string]methodHandler),
	}
}

// RegisterCall attaches a typed handler for one method. Must be called
// before Start. Re-registering the same method panics — that's a clear
// programmer error rather than a silent override.
func RegisterCall[Req, Resp any](s *CallServer, method string, handler func(Req) (Resp, error)) {
	if method == "" {
		panic("RegisterCall: empty method")
	}
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		panic("RegisterCall: server already started")
	}
	if _, exists := s.handlers[method]; exists {
		s.mu.Unlock()
		panic("RegisterCall: method " + method + " already registered")
	}
	s.handlers[method] = func(_ context.Context, payload json.RawMessage) (json.RawMessage, error) {
		var req Req
		if err := s.client.opts.codec.Decode(payload, &req); err != nil {
			return nil, fmt.Errorf("decode request: %w", err)
		}
		resp, err := handler(req)
		if err != nil {
			return nil, err
		}
		return s.client.opts.codec.Encode(resp)
	}
	s.mu.Unlock()
}

// Start joins the request channel to the client's shared BRPOP loop.
// Idempotent: repeated calls are no-ops.
func (s *CallServer) Start() {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return
	}
	s.started = true
	methods := make([]string, 0, len(s.handlers))
	for m := range s.handlers {
		methods = append(methods, m)
	}
	s.mu.Unlock()

	s.client.opts.logger.Info("call server starting", "channel", s.channel, "methods", methods)
	sub, err := s.client.queueMux.register(s.channel, s.dispatch)
	if err != nil {
		s.client.opts.logger.Error("call server registration failed", "channel", s.channel, "error", err)
		return
	}
	s.sub = sub
}

// Stop signals the server to stop accepting new requests and waits for
// in-flight handlers to finish.
func (s *CallServer) Stop() {
	s.stopMu.Lock()
	already := s.stopped
	s.stopped = true
	s.stopMu.Unlock()

	if !already && s.sub != nil {
		s.client.queueMux.unregister(s.sub)
	}
	s.inflight.Wait()
}

func (s *CallServer) isStopped() bool {
	s.stopMu.Lock()
	defer s.stopMu.Unlock()
	return s.stopped
}

func (s *CallServer) dispatch(payload string) {
	if s.isStopped() {
		return
	}

	var env callEnvelope
	if err := json.Unmarshal([]byte(payload), &env); err != nil {
		s.client.opts.logger.Error("envelope decode error", "channel", s.channel, "error", err)
		return
	}

	// Drop expired requests without replying.
	if env.Deadline > 0 && time.Now().UnixMilli() > env.Deadline {
		s.client.opts.logger.Debug("dropping expired call", "channel", s.channel, "id", env.ID, "method", env.Method)
		return
	}

	s.mu.RLock()
	handler, ok := s.handlers[env.Method]
	s.mu.RUnlock()

	if !ok {
		// Method-not-found: reply with an error rather than dropping
		// silently; the caller deserves to know.
		s.sendReply(env.ReplyChannel, callReply{OK: false, Error: "unknown method: " + env.Method})
		return
	}

	select {
	case s.sem <- struct{}{}:
	case <-s.client.ctx.Done():
		return
	}
	s.inflight.Add(1)
	go s.runHandler(env, handler)
}

func (s *CallServer) runHandler(env callEnvelope, handler methodHandler) {
	defer func() {
		<-s.sem
		s.inflight.Done()
	}()

	resp, err := handler(s.client.ctx, env.Payload)
	if err != nil {
		s.sendReply(env.ReplyChannel, callReply{OK: false, Error: err.Error()})
		return
	}
	s.sendReply(env.ReplyChannel, callReply{OK: true, Payload: resp})
}

func (s *CallServer) sendReply(channel string, reply callReply) {
	data, err := json.Marshal(reply)
	if err != nil {
		s.client.opts.logger.Error("reply encode failed", "channel", s.channel, "error", err)
		return
	}
	if err := s.client.redis.Publish(s.client.ctx, channel, data).Err(); err != nil {
		if s.client.ctx.Err() == nil {
			s.client.opts.logger.Error("reply publish failed", "channel", s.channel, "reply_channel", channel, "error", err)
		}
	}
}
