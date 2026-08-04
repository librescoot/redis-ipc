package redis_ipc

// mux.go: connection multiplexing.
//
// A Client keeps two shared connections instead of one per consumer:
//
//	pubsubMux: a single Redis pub/sub connection carrying every channel
//	the process subscribes to. go-redis dedicates a connection per PubSub
//	object, so the naive one-PubSub-per-watcher shape cost a socket (and a
//	health-check ping every 3s) per watched hash.
//
//	queueMux: a single BRPOP loop over every registered queue. BRPOP takes
//	a key list and reports which key popped, so N command queues need one
//	blocking connection, not N. This is the shape CallServer already used
//	for RPC methods, applied to the queue consumers.
//
// Both fan out to a per-consumer goroutine with a buffered channel, so a
// slow handler on one channel or queue cannot stall the others. That
// preserves the isolation the old one-connection-per-consumer shape had.

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// pubsubBacklog is the per-subscriber message backlog. Messages
	// beyond it make the dispatch loop wait for the slow subscriber.
	pubsubBacklog = 128

	// queueBacklog is the per-queue message backlog. Kept smaller than
	// pubsubBacklog because queue messages are commands, not state
	// notifications, and a deep backlog means something is already wrong.
	queueBacklog = 64

	// pubsubSendTimeout bounds how long the shared dispatch loop waits
	// for a wedged subscriber before dropping its message. Pub/sub is
	// lossy by nature (the payload is a field name, and the value is
	// re-fetched), so dropping beats stalling every other subscriber.
	pubsubSendTimeout = 30 * time.Second

	// stallWarning is how long a full backlog has to hold up the
	// dispatch loop before it is worth a log line. A burst that drains
	// in milliseconds is normal; one that lasts seconds is not.
	stallWarning = 5 * time.Second

	// subscribeAckTimeout bounds the wait for the server's SUBSCRIBE
	// confirmation.
	subscribeAckTimeout = 5 * time.Second

	// wakeKeyTTL keeps an orphaned wake key from outliving its process
	// by more than a few minutes.
	wakeKeyTTL = 5 * time.Minute
)

// ErrClientClosed is returned when registering a consumer on a client
// that is shutting down.
var ErrClientClosed = fmt.Errorf("redis-ipc: client is closed")

// ---------------------------------------------------------------------
// pub/sub multiplexing
// ---------------------------------------------------------------------

// ackWaiter signals that Redis confirmed a SUBSCRIBE. Reconnects replay
// the confirmation, hence the Once.
type ackWaiter struct {
	ch   chan struct{}
	once sync.Once
}

func (a *ackWaiter) signal() { a.once.Do(func() { close(a.ch) }) }

type pubsubMux struct {
	client *Client

	mu     sync.Mutex
	pubsub *redis.PubSub
	// subs is copy-on-write: the dispatch loop reads a slice header
	// under the lock and iterates it after releasing, so entries must
	// never be mutated in place.
	subs   map[string][]*pubsubSub
	acks   map[string]*ackWaiter
	closed bool
}

type pubsubSub struct {
	mux     *pubsubMux
	channel string
	handler func(payload string)

	ch   chan string
	gate chan struct{}
	stop chan struct{}

	gateOnce sync.Once
	stopOnce sync.Once
}

func newPubsubMux(c *Client) *pubsubMux {
	return &pubsubMux{
		client: c,
		subs:   make(map[string][]*pubsubSub),
		acks:   make(map[string]*ackWaiter),
	}
}

// subscribe registers handler for channel on the shared connection and
// blocks until Redis confirms the subscription. When gated is set the
// subscriber buffers incoming messages until release() is called.
// StartWithSync needs that window to fetch current state before handlers
// start seeing live updates.
func (m *pubsubMux) subscribe(channel string, gated bool, handler func(payload string)) (*pubsubSub, error) {
	s := &pubsubSub{
		mux:     m,
		channel: channel,
		handler: handler,
		ch:      make(chan string, pubsubBacklog),
		stop:    make(chan struct{}),
	}
	if gated {
		s.gate = make(chan struct{})
	}

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, ErrClientClosed
	}
	if m.pubsub == nil {
		m.pubsub = m.client.redis.Subscribe(m.client.ctx)
		m.client.wg.Add(1)
		go m.run(m.pubsub.ChannelWithSubscriptions())
	}

	existing := m.subs[channel]
	known := len(existing) > 0
	next := make([]*pubsubSub, len(existing), len(existing)+1)
	copy(next, existing)
	m.subs[channel] = append(next, s)

	ack := m.acks[channel]
	if ack == nil {
		ack = &ackWaiter{ch: make(chan struct{})}
		m.acks[channel] = ack
	}

	// SUBSCRIBE goes out under the lock so it cannot interleave with an
	// UNSUBSCRIBE for the same channel from a concurrent Stop(). The
	// write does not wait for the server's reply; that is what the ack
	// below is for, and it must be awaited with the lock released so
	// the dispatch loop can deliver it.
	var subErr error
	if !known {
		subErr = m.pubsub.Subscribe(m.client.ctx, channel)
	}
	m.mu.Unlock()

	m.client.wg.Add(1)
	go s.run()

	if subErr != nil {
		_ = m.unsubscribe(s)
		return nil, fmt.Errorf("subscribe to %s: %w", channel, subErr)
	}

	timer := time.NewTimer(subscribeAckTimeout)
	defer timer.Stop()

	select {
	case <-ack.ch:
		return s, nil
	case <-m.client.ctx.Done():
		_ = m.unsubscribe(s)
		return nil, m.client.ctx.Err()
	case <-timer.C:
		_ = m.unsubscribe(s)
		return nil, fmt.Errorf("subscription timeout for channel %s", channel)
	}
}

// unsubscribe drops one subscriber. The channel stays subscribed on the
// shared connection as long as any other subscriber still wants it.
func (m *pubsubMux) unsubscribe(s *pubsubSub) error {
	m.mu.Lock()
	found := false
	remaining := make([]*pubsubSub, 0, len(m.subs[s.channel]))
	for _, e := range m.subs[s.channel] {
		if e == s {
			found = true
			continue
		}
		remaining = append(remaining, e)
	}
	if !found {
		// Already gone, so make a repeated Stop() a real no-op instead of
		// re-sending UNSUBSCRIBE.
		m.mu.Unlock()
		s.halt()
		return nil
	}

	var err error
	if len(remaining) == 0 {
		delete(m.subs, s.channel)
		delete(m.acks, s.channel)
		// Under the lock, so it cannot race a concurrent subscribe to
		// the same channel.
		if m.pubsub != nil && !m.closed {
			err = m.pubsub.Unsubscribe(m.client.ctx, s.channel)
		}
	} else {
		m.subs[s.channel] = remaining
	}
	m.mu.Unlock()

	s.halt()
	return err
}

func (m *pubsubMux) run(ch <-chan any) {
	defer m.client.wg.Done()

	for msg := range ch {
		switch v := msg.(type) {
		case *redis.Subscription:
			if v.Kind == "subscribe" {
				m.ack(v.Channel)
			}
		case *redis.Message:
			m.dispatch(v.Channel, v.Payload)
		}
	}

	// The Go channel only closes when the PubSub does; stop every
	// subscriber goroutine so Close can complete.
	m.mu.Lock()
	subs := m.subs
	m.subs = make(map[string][]*pubsubSub)
	m.mu.Unlock()

	for _, list := range subs {
		for _, s := range list {
			s.halt()
		}
	}
}

func (m *pubsubMux) ack(channel string) {
	m.mu.Lock()
	ack := m.acks[channel]
	m.mu.Unlock()
	if ack != nil {
		ack.signal()
	}
}

func (m *pubsubMux) dispatch(channel, payload string) {
	m.mu.Lock()
	subs := m.subs[channel]
	m.mu.Unlock()

	for _, s := range subs {
		s.deliver(payload)
	}
}

func (m *pubsubMux) close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	ps := m.pubsub
	m.pubsub = nil
	subs := m.subs
	m.subs = make(map[string][]*pubsubSub)
	m.acks = make(map[string]*ackWaiter)
	m.mu.Unlock()

	for _, list := range subs {
		for _, s := range list {
			s.halt()
		}
	}

	if ps != nil {
		return ps.Close()
	}
	return nil
}

func (s *pubsubSub) deliver(payload string) {
	select {
	case s.ch <- payload:
		return
	case <-s.stop:
		return
	default:
	}

	warn := time.NewTimer(stallWarning)
	defer warn.Stop()
	drop := time.NewTimer(pubsubSendTimeout)
	defer drop.Stop()

	for {
		select {
		case s.ch <- payload:
			return
		case <-s.stop:
			return
		case <-s.mux.client.ctx.Done():
			return
		case <-warn.C:
			s.mux.client.opts.logger.Warn("pubsub subscriber is slow, dispatch waiting",
				"channel", s.channel, "backlog", pubsubBacklog)
		case <-drop.C:
			s.mux.client.opts.logger.Error("pubsub message dropped, subscriber stalled",
				"channel", s.channel, "timeout", pubsubSendTimeout)
			return
		}
	}
}

func (s *pubsubSub) run() {
	defer s.mux.client.wg.Done()

	if s.gate != nil {
		select {
		case <-s.gate:
		case <-s.stop:
			return
		}
	}

	for {
		select {
		case <-s.stop:
			return
		case payload := <-s.ch:
			s.handler(payload)
		}
	}
}

// release lets a gated subscriber start dispatching buffered messages.
func (s *pubsubSub) release() {
	if s.gate == nil {
		return
	}
	s.gateOnce.Do(func() { close(s.gate) })
}

func (s *pubsubSub) halt() {
	s.stopOnce.Do(func() { close(s.stop) })
}

// ---------------------------------------------------------------------
// BRPOP multiplexing
// ---------------------------------------------------------------------

type queueMux struct {
	client *Client

	mu       sync.Mutex
	subs     map[string][]*queueSub
	rotation int
	fanout   int
	started  bool
	closed   bool

	// wakeKey is a private list included in every BRPOP. Pushing to it
	// returns the blocking call early so the key set can be rebuilt
	// after a consumer registers or unregisters.
	wakeKey string
}

type queueSub struct {
	mux     *queueMux
	queue   string
	handler func(payload string)

	ch   chan string
	stop chan struct{}

	stopOnce sync.Once
}

func newQueueMux(c *Client) *queueMux {
	return &queueMux{
		client:  c,
		subs:    make(map[string][]*queueSub),
		wakeKey: "redis-ipc:wake:" + genCallID(),
	}
}

func (m *queueMux) register(queue string, handler func(payload string)) (*queueSub, error) {
	s := &queueSub{
		mux:     m,
		queue:   queue,
		handler: handler,
		ch:      make(chan string, queueBacklog),
		stop:    make(chan struct{}),
	}

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, ErrClientClosed
	}
	existing := m.subs[queue]
	next := make([]*queueSub, len(existing), len(existing)+1)
	copy(next, existing)
	m.subs[queue] = append(next, s)

	start := !m.started
	m.started = true
	m.mu.Unlock()

	m.client.wg.Add(1)
	go s.run()

	if start {
		m.client.wg.Add(1)
		go m.run()
	} else {
		m.wake()
	}
	return s, nil
}

func (m *queueMux) unregister(s *queueSub) {
	m.mu.Lock()
	found := false
	remaining := make([]*queueSub, 0, len(m.subs[s.queue]))
	for _, e := range m.subs[s.queue] {
		if e == s {
			found = true
			continue
		}
		remaining = append(remaining, e)
	}
	if !found {
		m.mu.Unlock()
		s.halt()
		return
	}
	if len(remaining) == 0 {
		delete(m.subs, s.queue)
	} else {
		m.subs[s.queue] = remaining
	}
	closed := m.closed
	m.mu.Unlock()

	s.halt()
	if !closed {
		m.wake()
	}
}

// pollKeys returns the wake key followed by every registered queue,
// rotated one position per call so a busy queue cannot starve the
// others: BRPOP always serves the earliest non-empty key.
func (m *queueMux) pollKeys() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	names := make([]string, 0, len(m.subs))
	for q := range m.subs {
		names = append(names, q)
	}
	sort.Strings(names)

	keys := make([]string, 0, len(names)+1)
	keys = append(keys, m.wakeKey)
	if n := len(names); n > 0 {
		off := m.rotation % n
		m.rotation++
		keys = append(keys, names[off:]...)
		keys = append(keys, names[:off]...)
	}
	return keys
}

func (m *queueMux) run() {
	defer m.client.wg.Done()

	for {
		if m.client.ctx.Err() != nil || m.isClosed() {
			m.client.opts.logger.Debug("queue mux shutting down")
			return
		}

		keys := m.pollKeys()
		result, err := m.client.redis.BRPop(m.client.ctx, brpopBlockingTimeout, keys...).Result()
		if err == redis.Nil {
			continue
		}
		if err != nil {
			if m.client.ctx.Err() != nil || m.isClosed() {
				return
			}
			// Transient read errors (e.g. i/o timeout during brief Redis
			// stalls) are expected here; nothing is lost, the messages
			// stay in Redis until the next BRPOP.
			m.client.opts.logger.Warn("BRPOP error, retrying", "error", err, "delay", "5s")
			select {
			case <-m.client.ctx.Done():
				return
			case <-time.After(5 * time.Second):
			}
			continue
		}
		if len(result) < 2 || result[0] == m.wakeKey {
			continue
		}
		m.dispatch(result[0], result[1])
	}
}

func (m *queueMux) dispatch(queue, payload string) {
	m.mu.Lock()
	subs := m.subs[queue]
	var target *queueSub
	if n := len(subs); n > 0 {
		// More than one consumer on a queue is unusual, but a message
		// still belongs to exactly one of them, so round-robin rather
		// than fan out, matching the old BRPOP-per-consumer split.
		target = subs[m.fanout%n]
		m.fanout++
	}
	m.mu.Unlock()

	if target == nil {
		m.client.opts.logger.Warn("dropping message for unregistered queue", "queue", queue)
		return
	}
	target.deliver(payload)
}

func (m *queueMux) isClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

// wake pushes a token onto the private wake key so a blocked BRPOP
// returns and picks up the new key set.
func (m *queueMux) wake() {
	ctx := m.client.ctx
	if ctx.Err() != nil {
		return
	}
	pipe := m.client.redis.Pipeline()
	pipe.LPush(ctx, m.wakeKey, "wake")
	pipe.Expire(ctx, m.wakeKey, wakeKeyTTL)
	if _, err := pipe.Exec(ctx); err != nil && ctx.Err() == nil {
		m.client.opts.logger.Warn("queue mux wake failed", "error", err)
	}
}

func (m *queueMux) close() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true
	subs := m.subs
	m.subs = make(map[string][]*queueSub)
	started := m.started
	m.mu.Unlock()

	for _, list := range subs {
		for _, s := range list {
			s.halt()
		}
	}

	// The BRPOP loop unblocks on context cancellation; all that is left
	// is not leaving the wake key behind.
	if started {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := m.client.redis.Del(ctx, m.wakeKey).Err(); err != nil {
			m.client.opts.logger.Debug("wake key cleanup failed", "key", m.wakeKey, "error", err)
		}
	}
}

// deliver hands a message to the consumer goroutine. Unlike pub/sub this
// never drops: queue messages are commands, and Redis holds the rest of
// the backlog until the consumer catches up.
func (s *queueSub) deliver(payload string) {
	select {
	case s.ch <- payload:
		return
	case <-s.stop:
		return
	default:
	}

	warn := time.NewTimer(stallWarning)
	defer warn.Stop()

	for {
		select {
		case s.ch <- payload:
			return
		case <-s.stop:
			return
		case <-s.mux.client.ctx.Done():
			return
		case <-warn.C:
			s.mux.client.opts.logger.Warn("queue consumer is slow, dispatch waiting",
				"queue", s.queue, "backlog", queueBacklog)
		}
	}
}

func (s *queueSub) run() {
	defer s.mux.client.wg.Done()

	for {
		select {
		case <-s.stop:
			return
		case payload := <-s.ch:
			s.handler(payload)
		}
	}
}

func (s *queueSub) halt() {
	s.stopOnce.Do(func() { close(s.stop) })
}
