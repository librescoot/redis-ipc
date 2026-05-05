package redis_ipc

// call.go — typed RPC over Redis.
//
// Wire protocol:
//   Client: SUBSCRIBE <channel>:reply:<id>; LPUSH <channel> <envelope>;
//           wait for one message on the reply subscription.
//   Server: BRPOP <channel>; PUBLISH <reply-channel> <reply>.
//
// Pub/sub for the reply leg is deliberate — it gives sub-second timeouts
// (BLPop in go-redis v9 hard-floors at 1s) and avoids per-call key
// bookkeeping. If the caller has timed out and unsubscribed, the
// server's PUBLISH simply has no subscriber, which is the desired
// behaviour.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// ErrCallTimeout is returned by Call when no response arrives within
// the caller-specified timeout.
var ErrCallTimeout = errors.New("redis-ipc: call timed out")

// CallError wraps an error returned by a HandleCalls handler.
type CallError struct{ Msg string }

func (e *CallError) Error() string { return e.Msg }

// IsCallError reports whether err originated as a server-side handler error.
func IsCallError(err error) bool {
	var ce *CallError
	return errors.As(err, &ce)
}

type callEnvelope struct {
	ID           string          `json:"id"`
	Method       string          `json:"method,omitempty"` // empty for one-channel-per-RPC (Call); populated for CallServer dispatch
	ReplyChannel string          `json:"reply_channel"`
	Deadline     int64           `json:"deadline"`
	Payload      json.RawMessage `json:"payload"`
}

type callReply struct {
	OK      bool            `json:"ok"`
	Payload json.RawMessage `json:"payload,omitempty"`
	Error   string          `json:"error,omitempty"`
}

func genCallID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// Call sends a typed request to a Redis list-backed RPC channel and
// blocks until the response arrives or the timeout elapses. Returns
// ErrCallTimeout on timeout, *CallError on a server-side handler error,
// or a wrapped error for transport / decode failures.
func Call[Req, Resp any](c *Client, channel string, req Req, timeout time.Duration) (Resp, error) {
	var zero Resp

	payload, err := c.opts.codec.Encode(req)
	if err != nil {
		return zero, fmt.Errorf("encode request: %w", err)
	}

	id := genCallID()
	replyChannel := channel + ":reply:" + id
	env := callEnvelope{
		ID:           id,
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

	// Wait for the SUBSCRIBE confirmation before sending the request.
	// Receive returns the *Subscription confirmation as the first
	// message; without this, the request could race ahead of the
	// subscriber and the reply would be lost.
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

// CallHandlerOption configures a CallHandler.
type CallHandlerOption func(*callHandlerOpts)

type callHandlerOpts struct {
	concurrency int
}

// WithCallConcurrency caps the number of in-flight handler invocations.
// Defaults to 4.
func WithCallConcurrency(n int) CallHandlerOption {
	return func(o *callHandlerOpts) {
		if n > 0 {
			o.concurrency = n
		}
	}
}

// CallHandler is the server side of a Call channel. Stop() drains
// in-flight handlers before returning.
type CallHandler[Req, Resp any] struct {
	client   *Client
	channel  string
	handler  func(Req) (Resp, error)
	sem      chan struct{}
	stopped  bool
	stopMu   sync.Mutex
	inflight sync.WaitGroup
}

// HandleCalls registers a typed RPC handler. Each incoming request is
// dispatched to a goroutine bounded by the concurrency option.
func HandleCalls[Req, Resp any](c *Client, channel string, handler func(Req) (Resp, error), opts ...CallHandlerOption) *CallHandler[Req, Resp] {
	o := &callHandlerOpts{concurrency: 4}
	for _, opt := range opts {
		opt(o)
	}
	h := &CallHandler[Req, Resp]{
		client:  c,
		channel: channel,
		handler: handler,
		sem:     make(chan struct{}, o.concurrency),
	}
	c.wg.Add(1)
	go h.processLoopWithRestart()
	return h
}

// Stop signals the handler to stop accepting new requests and waits
// for in-flight handlers to finish.
func (h *CallHandler[Req, Resp]) Stop() {
	h.stopMu.Lock()
	h.stopped = true
	h.stopMu.Unlock()
	h.inflight.Wait()
}

func (h *CallHandler[Req, Resp]) isStopped() bool {
	h.stopMu.Lock()
	defer h.stopMu.Unlock()
	return h.stopped
}

func (h *CallHandler[Req, Resp]) processLoopWithRestart() {
	defer h.client.wg.Done()
	for {
		if h.client.ctx.Err() != nil || h.isStopped() {
			h.client.opts.logger.Info("call handler shutting down", "channel", h.channel)
			return
		}
		h.processLoop()
		if h.client.ctx.Err() != nil || h.isStopped() {
			return
		}
		h.client.opts.logger.Info("call handler restarting", "channel", h.channel, "delay", "5s")
		time.Sleep(5 * time.Second)
	}
}

func (h *CallHandler[Req, Resp]) processLoop() {
	for {
		if h.isStopped() {
			return
		}
		result, err := h.client.redis.BRPop(h.client.ctx, brpopBlockingTimeout, h.channel).Result()
		if err == redis.Nil {
			continue
		}
		if err != nil {
			if h.client.ctx.Err() != nil {
				return
			}
			h.client.opts.logger.Warn("BRPOP error, restarting call handler", "channel", h.channel, "error", err)
			return
		}
		if len(result) < 2 {
			continue
		}
		var env callEnvelope
		if err := json.Unmarshal([]byte(result[1]), &env); err != nil {
			h.client.opts.logger.Error("envelope decode error", "channel", h.channel, "error", err)
			continue
		}
		// Drop expired requests without replying — caller already gave up.
		if env.Deadline > 0 && time.Now().UnixMilli() > env.Deadline {
			h.client.opts.logger.Debug("dropping expired call", "channel", h.channel, "id", env.ID)
			continue
		}
		// Acquire semaphore — natural backpressure on BRPOP.
		select {
		case h.sem <- struct{}{}:
		case <-h.client.ctx.Done():
			return
		}
		h.inflight.Add(1)
		go h.runHandler(env)
	}
}

func (h *CallHandler[Req, Resp]) runHandler(env callEnvelope) {
	defer func() {
		<-h.sem
		h.inflight.Done()
	}()

	var req Req
	if err := h.client.opts.codec.Decode(env.Payload, &req); err != nil {
		h.sendReply(env.ReplyChannel, callReply{OK: false, Error: "decode request: " + err.Error()})
		return
	}

	resp, err := h.handler(req)
	if err != nil {
		h.sendReply(env.ReplyChannel, callReply{OK: false, Error: err.Error()})
		return
	}

	payload, err := h.client.opts.codec.Encode(resp)
	if err != nil {
		h.sendReply(env.ReplyChannel, callReply{OK: false, Error: "encode response: " + err.Error()})
		return
	}
	h.sendReply(env.ReplyChannel, callReply{OK: true, Payload: payload})
}

func (h *CallHandler[Req, Resp]) sendReply(channel string, reply callReply) {
	data, err := json.Marshal(reply)
	if err != nil {
		h.client.opts.logger.Error("reply encode failed", "channel", h.channel, "error", err)
		return
	}
	if err := h.client.redis.Publish(h.client.ctx, channel, data).Err(); err != nil {
		if h.client.ctx.Err() == nil {
			h.client.opts.logger.Error("reply publish failed", "channel", h.channel, "reply_channel", channel, "error", err)
		}
	}
}
