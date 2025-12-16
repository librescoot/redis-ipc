package redis_ipc

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Codec handles message serialization/deserialization
type Codec interface {
	Encode(v any) ([]byte, error)
	Decode(data []byte, v any) error
}

// JSONCodec is the default codec using JSON
type JSONCodec struct{}

func (JSONCodec) Encode(v any) ([]byte, error)       { return json.Marshal(v) }
func (JSONCodec) Decode(data []byte, v any) error    { return json.Unmarshal(data, v) }

// StringCodec passes strings through unchanged
type StringCodec struct{}

func (StringCodec) Encode(v any) ([]byte, error) {
	switch val := v.(type) {
	case string:
		return []byte(val), nil
	case []byte:
		return val, nil
	default:
		return nil, fmt.Errorf("StringCodec: expected string or []byte, got %T", v)
	}
}

func (StringCodec) Decode(data []byte, v any) error {
	switch ptr := v.(type) {
	case *string:
		*ptr = string(data)
		return nil
	case *[]byte:
		*ptr = data
		return nil
	default:
		return fmt.Errorf("StringCodec: expected *string or *[]byte, got %T", v)
	}
}

// options holds client configuration
type options struct {
	address       string
	port          int
	retryInterval time.Duration
	maxRetries    int
	poolSize      int
	dialTimeout   time.Duration
	logger        *slog.Logger
	codec         Codec
	onConnect     func()
	onDisconnect  func(error)
}

func defaultOptions() *options {
	return &options{
		address:       "localhost",
		port:          6379,
		retryInterval: 5 * time.Second,
		maxRetries:    3,
		poolSize:      3,
		dialTimeout:   2 * time.Second,
		logger:        slog.New(slog.NewTextHandler(os.Stderr, nil)),
		codec:         JSONCodec{},
	}
}

// Option configures the client
type Option func(*options)

// WithAddress sets the Redis server address
func WithAddress(addr string) Option {
	return func(o *options) { o.address = addr }
}

// WithPort sets the Redis server port
func WithPort(port int) Option {
	return func(o *options) { o.port = port }
}

// WithRetryInterval sets the connection monitoring interval
func WithRetryInterval(d time.Duration) Option {
	return func(o *options) { o.retryInterval = d }
}

// WithMaxRetries sets the maximum number of retries for commands
func WithMaxRetries(n int) Option {
	return func(o *options) { o.maxRetries = n }
}

// WithPoolSize sets the connection pool size
func WithPoolSize(n int) Option {
	return func(o *options) { o.poolSize = n }
}

// WithDialTimeout sets the connection timeout
func WithDialTimeout(d time.Duration) Option {
	return func(o *options) { o.dialTimeout = d }
}

// WithLogger sets a custom logger
func WithLogger(logger *slog.Logger) Option {
	return func(o *options) { o.logger = logger }
}

// WithCodec sets the message codec (default: JSONCodec)
func WithCodec(codec Codec) Option {
	return func(o *options) { o.codec = codec }
}

// WithOnConnect sets a callback invoked when connection is established
func WithOnConnect(fn func()) Option {
	return func(o *options) { o.onConnect = fn }
}

// WithOnDisconnect sets a callback invoked when connection is lost
func WithOnDisconnect(fn func(error)) Option {
	return func(o *options) { o.onDisconnect = fn }
}

// Client is the main Redis IPC client
type Client struct {
	opts        *options
	redis       *redis.Client
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	connected   bool
	connMu      sync.RWMutex
	subGroups   sync.Map
	reqHandlers sync.Map
}

// New creates a new Redis IPC client with functional options
func New(opts ...Option) (*Client, error) {
	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}

	ctx, cancel := context.WithCancel(context.Background())

	redisOpts := &redis.Options{
		Addr:        fmt.Sprintf("%s:%d", o.address, o.port),
		DialTimeout: o.dialTimeout,
		PoolSize:    o.poolSize,
		MaxRetries:  o.maxRetries,
	}

	rc := redis.NewClient(redisOpts)

	c := &Client{
		opts:   o,
		redis:  rc,
		ctx:    ctx,
		cancel: cancel,
	}

	if err := rc.Ping(ctx).Err(); err != nil {
		cancel()
		return nil, fmt.Errorf("redis connect failed: %w", err)
	}

	c.setConnected(true)
	if o.onConnect != nil {
		o.onConnect()
	}

	c.wg.Add(1)
	go c.monitorConnection()

	return c, nil
}

func (c *Client) setConnected(connected bool) {
	c.connMu.Lock()
	wasConnected := c.connected
	c.connected = connected
	c.connMu.Unlock()

	if wasConnected && !connected && c.opts.onDisconnect != nil {
		c.opts.onDisconnect(nil)
	} else if !wasConnected && connected && c.opts.onConnect != nil {
		c.opts.onConnect()
	}
}

// Connected returns the current connection status
func (c *Client) Connected() bool {
	c.connMu.RLock()
	defer c.connMu.RUnlock()
	return c.connected
}

// Logger returns the client's logger
func (c *Client) Logger() *slog.Logger {
	return c.opts.logger
}

// Codec returns the client's codec
func (c *Client) Codec() Codec {
	return c.opts.codec
}

func (c *Client) monitorConnection() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.opts.retryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			err := c.redis.Ping(context.Background()).Err()
			if err != nil {
				c.opts.logger.Error("connection check failed", "error", err)
				c.setConnected(false)
			} else {
				c.setConnected(true)
			}
		}
	}
}

// Close gracefully shuts down the client, waiting for handlers to finish
func (c *Client) Close() error {
	return c.CloseWithTimeout(30 * time.Second)
}

// CloseWithTimeout shuts down the client with a maximum wait time
func (c *Client) CloseWithTimeout(timeout time.Duration) error {
	c.cancel()

	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All goroutines finished
	case <-time.After(timeout):
		c.opts.logger.Warn("shutdown timeout, some handlers may not have finished", "timeout", timeout)
	}

	return c.redis.Close()
}

// Subscribe creates or retrieves a typed subscription
func Subscribe[T any](c *Client, channel string, handler func(T) error) (*Subscription[T], error) {
	sub := &Subscription[T]{
		client:  c,
		channel: channel,
		handler: handler,
	}
	return sub, sub.start()
}

// Subscription is a typed channel subscription
type Subscription[T any] struct {
	client  *Client
	channel string
	handler func(T) error
	pubsub  *redis.PubSub
}

func (s *Subscription[T]) start() error {
	s.pubsub = s.client.redis.Subscribe(s.client.ctx, s.channel)

	subscribed := make(chan struct{})
	s.client.wg.Add(1)
	go func() {
		defer s.client.wg.Done()
		ch := s.pubsub.Channel()
		close(subscribed)
		for msg := range ch {
			var val T
			if err := s.client.opts.codec.Decode([]byte(msg.Payload), &val); err != nil {
				s.client.opts.logger.Error("decode error", "channel", s.channel, "error", err)
				continue
			}
			if err := s.handler(val); err != nil {
				s.client.opts.logger.Error("handler error", "channel", s.channel, "error", err)
			}
		}
	}()

	select {
	case <-subscribed:
		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("subscription timeout for channel %s", s.channel)
	}
}

// Unsubscribe stops the subscription
func (s *Subscription[T]) Unsubscribe() error {
	if s.pubsub != nil {
		return s.pubsub.Close()
	}
	return nil
}

// HandleRequests creates a typed queue handler
func HandleRequests[T any](c *Client, queue string, handler func(T) error) *QueueHandler[T] {
	qh := &QueueHandler[T]{
		client:  c,
		queue:   queue,
		handler: handler,
	}
	c.wg.Add(1)
	go qh.processLoopWithRestart()
	return qh
}

// QueueHandler processes messages from a Redis list
type QueueHandler[T any] struct {
	client  *Client
	queue   string
	handler func(T) error
	stopped bool
	stopMu  sync.Mutex
}

// Stop stops the queue handler
func (qh *QueueHandler[T]) Stop() {
	qh.stopMu.Lock()
	qh.stopped = true
	qh.stopMu.Unlock()
}

func (qh *QueueHandler[T]) isStopped() bool {
	qh.stopMu.Lock()
	defer qh.stopMu.Unlock()
	return qh.stopped
}

func (qh *QueueHandler[T]) processLoopWithRestart() {
	defer qh.client.wg.Done()

	for {
		if qh.client.ctx.Err() != nil || qh.isStopped() {
			qh.client.opts.logger.Info("queue handler shutting down", "queue", qh.queue)
			return
		}

		qh.client.opts.logger.Debug("starting queue handler", "queue", qh.queue)
		qh.processLoop()

		if qh.client.ctx.Err() != nil || qh.isStopped() {
			return
		}

		qh.client.opts.logger.Info("queue handler restarting", "queue", qh.queue, "delay", "5s")
		time.Sleep(5 * time.Second)
	}
}

func (qh *QueueHandler[T]) processLoop() {
	for {
		if qh.isStopped() {
			return
		}

		result, err := qh.client.redis.BRPop(qh.client.ctx, 1*time.Second, qh.queue).Result()
		if err == redis.Nil {
			continue
		}
		if err != nil {
			if qh.client.ctx.Err() != nil {
				return
			}
			qh.client.opts.logger.Error("BRPOP error", "queue", qh.queue, "error", err)
			return
		}

		var val T
		if err := qh.client.opts.codec.Decode([]byte(result[1]), &val); err != nil {
			qh.client.opts.logger.Error("decode error", "queue", qh.queue, "error", err)
			continue
		}

		if err := qh.handler(val); err != nil {
			qh.client.opts.logger.Error("handler error", "queue", qh.queue, "error", err)
		}
	}
}

// Router provides message-type based routing for a channel
type Router struct {
	client   *Client
	channel  string
	handlers map[string]func(json.RawMessage) error
	mu       sync.RWMutex
	pubsub   *redis.PubSub
}

// Envelope is the standard message format for routed messages
type Envelope struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

// NewRouter creates a message router for a channel
func (c *Client) NewRouter(channel string) *Router {
	return &Router{
		client:   c,
		channel:  channel,
		handlers: make(map[string]func(json.RawMessage) error),
	}
}

// Handle registers a typed handler for a message type
func Handle[T any](r *Router, msgType string, handler func(T) error) *Router {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.handlers[msgType] = func(data json.RawMessage) error {
		var val T
		if err := json.Unmarshal(data, &val); err != nil {
			return fmt.Errorf("decode %s: %w", msgType, err)
		}
		return handler(val)
	}
	return r
}

// Start begins listening for messages
func (r *Router) Start() error {
	r.pubsub = r.client.redis.Subscribe(r.client.ctx, r.channel)

	subscribed := make(chan struct{})
	r.client.wg.Add(1)
	go func() {
		defer r.client.wg.Done()
		ch := r.pubsub.Channel()
		close(subscribed)

		for msg := range ch {
			var env Envelope
			if err := json.Unmarshal([]byte(msg.Payload), &env); err != nil {
				r.client.opts.logger.Error("envelope decode error", "channel", r.channel, "error", err)
				continue
			}

			r.mu.RLock()
			handler, ok := r.handlers[env.Type]
			r.mu.RUnlock()

			if !ok {
				r.client.opts.logger.Warn("no handler for message type", "channel", r.channel, "type", env.Type)
				continue
			}

			if err := handler(env.Data); err != nil {
				r.client.opts.logger.Error("handler error", "channel", r.channel, "type", env.Type, "error", err)
			}
		}
	}()

	select {
	case <-subscribed:
		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("router subscription timeout for channel %s", r.channel)
	}
}

// Stop stops the router
func (r *Router) Stop() error {
	if r.pubsub != nil {
		return r.pubsub.Close()
	}
	return nil
}

// PublishRouted publishes a message with type routing
func PublishRouted[T any](c *Client, ctx context.Context, channel, msgType string, data T) error {
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("marshal data: %w", err)
	}

	env := Envelope{
		Type: msgType,
		Data: dataBytes,
	}

	envBytes, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}

	return c.redis.Publish(ctx, channel, envBytes).Err()
}

// TxGroup batches Redis commands for atomic execution
type TxGroup struct {
	client *Client
	pipe   redis.Pipeliner
}

// NewTxGroup creates a new transaction group
func (c *Client) NewTxGroup() *TxGroup {
	return &TxGroup{
		client: c,
		pipe:   c.redis.Pipeline(),
	}
}

// Add queues a Redis command
func (g *TxGroup) Add(cmd string, args ...interface{}) *TxGroup {
	cmdArgs := append([]interface{}{cmd}, args...)
	g.pipe.Do(g.client.ctx, cmdArgs...)
	return g
}

// Exec executes all queued commands and returns results
func (g *TxGroup) Exec(ctx context.Context) ([]interface{}, error) {
	cmders, err := g.pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return nil, err
	}

	results := make([]interface{}, len(cmders))
	for i, cmder := range cmders {
		val, err := cmder.(*redis.Cmd).Result()
		if err != nil && err != redis.Nil {
			results[i] = err
			continue
		}
		results[i] = val
	}

	g.pipe = g.client.redis.Pipeline()
	return results, nil
}

// Direct Redis operations with context

// Do executes a Redis command
func (c *Client) Do(ctx context.Context, cmd string, args ...interface{}) (interface{}, error) {
	cmdArgs := append([]interface{}{cmd}, args...)
	return c.redis.Do(ctx, cmdArgs...).Result()
}

// Get retrieves the value of a key
func (c *Client) Get(ctx context.Context, key string) (string, error) {
	return c.redis.Get(ctx, key).Result()
}

// Set sets the value of a key
func (c *Client) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	return c.redis.Set(ctx, key, value, expiration).Err()
}

// HGet retrieves the value of a hash field
func (c *Client) HGet(ctx context.Context, key, field string) (string, error) {
	return c.redis.HGet(ctx, key, field).Result()
}

// HSet sets the value of a hash field
func (c *Client) HSet(ctx context.Context, key, field string, value interface{}) error {
	return c.redis.HSet(ctx, key, field, value).Err()
}

// HGetAll retrieves all fields and values of a hash
func (c *Client) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	return c.redis.HGetAll(ctx, key).Result()
}

// LPush inserts values at the head of a list
func (c *Client) LPush(ctx context.Context, key string, values ...interface{}) (int64, error) {
	return c.redis.LPush(ctx, key, values...).Result()
}

// RPush inserts values at the tail of a list
func (c *Client) RPush(ctx context.Context, key string, values ...interface{}) (int64, error) {
	return c.redis.RPush(ctx, key, values...).Result()
}

// LPop removes and returns the first element of a list
func (c *Client) LPop(ctx context.Context, key string) (string, error) {
	return c.redis.LPop(ctx, key).Result()
}

// RPop removes and returns the last element of a list
func (c *Client) RPop(ctx context.Context, key string) (string, error) {
	return c.redis.RPop(ctx, key).Result()
}

// BLPop blocks until it can remove and return the first element of a list
func (c *Client) BLPop(ctx context.Context, timeout time.Duration, keys ...string) ([]string, error) {
	return c.redis.BLPop(ctx, timeout, keys...).Result()
}

// BRPop blocks until it can remove and return the last element of a list
func (c *Client) BRPop(ctx context.Context, timeout time.Duration, keys ...string) ([]string, error) {
	return c.redis.BRPop(ctx, timeout, keys...).Result()
}

// Publish publishes a message to a channel
func (c *Client) Publish(ctx context.Context, channel string, message interface{}) (int64, error) {
	return c.redis.Publish(ctx, channel, message).Result()
}

// PublishTyped publishes a typed message using the client's codec
func PublishTyped[T any](c *Client, ctx context.Context, channel string, message T) error {
	data, err := c.opts.codec.Encode(message)
	if err != nil {
		return fmt.Errorf("encode message: %w", err)
	}
	return c.redis.Publish(ctx, channel, data).Err()
}

// SendRequest sends a typed message to a queue
func SendRequest[T any](c *Client, ctx context.Context, queue string, message T) error {
	data, err := c.opts.codec.Encode(message)
	if err != nil {
		return fmt.Errorf("encode message: %w", err)
	}
	return c.redis.LPush(ctx, queue, data).Err()
}

// Incr increments the integer value of a key by one
func (c *Client) Incr(ctx context.Context, key string) (int64, error) {
	return c.redis.Incr(ctx, key).Result()
}

// Decr decrements the integer value of a key by one
func (c *Client) Decr(ctx context.Context, key string) (int64, error) {
	return c.redis.Decr(ctx, key).Result()
}

// Exists checks if keys exist
func (c *Client) Exists(ctx context.Context, keys ...string) (int64, error) {
	return c.redis.Exists(ctx, keys...).Result()
}

// Del deletes keys
func (c *Client) Del(ctx context.Context, keys ...string) (int64, error) {
	return c.redis.Del(ctx, keys...).Result()
}

// Expire sets a key's time to live
func (c *Client) Expire(ctx context.Context, key string, expiration time.Duration) (bool, error) {
	return c.redis.Expire(ctx, key, expiration).Result()
}

// Raw returns the underlying go-redis client for advanced operations
func (c *Client) Raw() *redis.Client {
	return c.redis
}
