package redis_ipc

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// StreamPublisher publishes messages to a Redis stream using XADD.
type StreamPublisher struct {
	client *Client
	stream string
	maxLen int64
}

// streamPublisherOptions holds configuration for StreamPublisher
type streamPublisherOptions struct {
	maxLen int64
}

// StreamPublisherOption configures a StreamPublisher
type StreamPublisherOption func(*streamPublisherOptions)

// WithMaxLen sets the maximum stream length (MAXLEN option)
func WithMaxLen(n int64) StreamPublisherOption {
	return func(o *streamPublisherOptions) { o.maxLen = n }
}

// NewStreamPublisher creates a publisher for a Redis stream.
func (c *Client) NewStreamPublisher(stream string, opts ...StreamPublisherOption) *StreamPublisher {
	o := &streamPublisherOptions{
		maxLen: 1000, // default
	}
	for _, opt := range opts {
		opt(o)
	}

	return &StreamPublisher{
		client: c,
		stream: stream,
		maxLen: o.maxLen,
	}
}

// Add publishes a message to the stream with auto-generated ID.
// Returns the generated message ID.
func (sp *StreamPublisher) Add(values map[string]any) (string, error) {
	return sp.client.redis.XAdd(sp.client.Context(), &redis.XAddArgs{
		Stream: sp.stream,
		MaxLen: sp.maxLen,
		Approx: true, // Use ~ for efficiency
		ID:     "*",
		Values: values,
	}).Result()
}

// AddWithID publishes a message with a specific ID.
func (sp *StreamPublisher) AddWithID(id string, values map[string]any) (string, error) {
	return sp.client.redis.XAdd(sp.client.Context(), &redis.XAddArgs{
		Stream: sp.stream,
		MaxLen: sp.maxLen,
		Approx: true,
		ID:     id,
		Values: values,
	}).Result()
}

// Stream returns the stream name.
func (sp *StreamPublisher) Stream() string {
	return sp.stream
}

// StreamAdd publishes a typed message to the stream using JSON encoding.
func StreamAdd[T any](sp *StreamPublisher, msg T) (string, error) {
	data, err := json.Marshal(msg)
	if err != nil {
		return "", fmt.Errorf("marshal message: %w", err)
	}

	// Convert JSON to map for Redis
	var values map[string]any
	if err := json.Unmarshal(data, &values); err != nil {
		return "", fmt.Errorf("unmarshal to map: %w", err)
	}

	return sp.Add(values)
}

// StreamConsumer consumes messages from a Redis stream using XREAD.
type StreamConsumer struct {
	client       *Client
	stream       string
	handler      func(id string, values map[string]string) error
	blockTimeout time.Duration
	group        string
	consumer     string
	mu           sync.Mutex
	running      bool
}

// streamConsumerOptions holds configuration for StreamConsumer
type streamConsumerOptions struct {
	blockTimeout time.Duration
	group        string
	consumer     string
}

// StreamConsumerOption configures a StreamConsumer
type StreamConsumerOption func(*streamConsumerOptions)

// WithBlockTimeout sets the XREAD block timeout
func WithBlockTimeout(d time.Duration) StreamConsumerOption {
	return func(o *streamConsumerOptions) { o.blockTimeout = d }
}

// WithConsumerGroup configures consumer group mode (XREADGROUP)
func WithConsumerGroup(group, consumer string) StreamConsumerOption {
	return func(o *streamConsumerOptions) {
		o.group = group
		o.consumer = consumer
	}
}

// NewStreamConsumer creates a consumer for a Redis stream.
func (c *Client) NewStreamConsumer(stream string, opts ...StreamConsumerOption) *StreamConsumer {
	o := &streamConsumerOptions{
		blockTimeout: 1 * time.Second,
	}
	for _, opt := range opts {
		opt(o)
	}

	return &StreamConsumer{
		client:       c,
		stream:       stream,
		blockTimeout: o.blockTimeout,
		group:        o.group,
		consumer:     o.consumer,
	}
}

// Handle sets the message handler function.
func (sc *StreamConsumer) Handle(handler func(id string, values map[string]string) error) *StreamConsumer {
	sc.handler = handler
	return sc
}

// Start begins consuming messages from the stream.
// startID can be:
//   - "$" to read only new messages
//   - "0" to read from the beginning
//   - A specific message ID to read after that ID
//   - ">" for consumer groups (only undelivered messages)
func (sc *StreamConsumer) Start(startID string) error {
	if sc.handler == nil {
		return fmt.Errorf("no handler set")
	}

	sc.mu.Lock()
	if sc.running {
		sc.mu.Unlock()
		return fmt.Errorf("consumer already running")
	}
	sc.running = true
	sc.mu.Unlock()

	// Create consumer group if using group mode
	if sc.group != "" {
		// Try to create the group, ignore error if it already exists
		ctx := sc.client.Context()
		err := sc.client.redis.XGroupCreateMkStream(ctx, sc.stream, sc.group, "0").Err()
		if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
			sc.client.opts.logger.Warn("failed to create consumer group", "error", err)
		}
	}

	sc.client.wg.Add(1)
	go sc.consumeLoop(startID)

	return nil
}

func (sc *StreamConsumer) consumeLoop(startID string) {
	defer sc.client.wg.Done()
	defer func() {
		sc.mu.Lock()
		sc.running = false
		sc.mu.Unlock()
	}()

	lastID := startID

	for {
		select {
		case <-sc.client.ctx.Done():
			return
		default:
		}

		ctx := sc.client.Context()
		var streams []redis.XStream
		var err error

		if sc.group != "" {
			// Consumer group mode
			streams, err = sc.client.redis.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    sc.group,
				Consumer: sc.consumer,
				Streams:  []string{sc.stream, lastID},
				Count:    10,
				Block:    sc.blockTimeout,
			}).Result()
		} else {
			// Simple XREAD mode
			streams, err = sc.client.redis.XRead(ctx, &redis.XReadArgs{
				Streams: []string{sc.stream, lastID},
				Count:   10,
				Block:   sc.blockTimeout,
			}).Result()
		}

		if err != nil {
			if err == redis.Nil {
				continue // No messages, keep waiting
			}
			if sc.client.ctx.Err() != nil {
				return
			}
			sc.client.opts.logger.Error("XREAD error", "stream", sc.stream, "error", err)
			time.Sleep(1 * time.Second) // Back off on error
			continue
		}

		for _, stream := range streams {
			for _, msg := range stream.Messages {
				if err := sc.handler(msg.ID, stringifyValues(msg.Values)); err != nil {
					sc.client.opts.logger.Error("handler error", "stream", sc.stream, "id", msg.ID, "error", err)
				}

				// Acknowledge message in consumer group mode
				if sc.group != "" {
					if err := sc.client.redis.XAck(ctx, sc.stream, sc.group, msg.ID).Err(); err != nil {
						sc.client.opts.logger.Error("XACK error", "stream", sc.stream, "id", msg.ID, "error", err)
					}
				}

				// Update lastID for next read (only in simple mode)
				if sc.group == "" {
					lastID = msg.ID
				}
			}
		}
	}
}

// Stop stops the consumer (by cancelling the context passed to Start).
// Note: The consumer stops when the context passed to Start is cancelled.
func (sc *StreamConsumer) Stop() {
	sc.mu.Lock()
	sc.running = false
	sc.mu.Unlock()
}

// Running returns whether the consumer is currently running.
func (sc *StreamConsumer) Running() bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.running
}

// StreamHandle registers a typed handler for stream messages.
func StreamHandle[T any](sc *StreamConsumer, handler func(id string, msg T) error) *StreamConsumer {
	sc.handler = func(id string, values map[string]string) error {
		// Convert map to JSON then to struct
		data, err := json.Marshal(values)
		if err != nil {
			return fmt.Errorf("marshal values: %w", err)
		}

		var msg T
		if err := json.Unmarshal(data, &msg); err != nil {
			return fmt.Errorf("unmarshal message: %w", err)
		}

		return handler(id, msg)
	}
	return sc
}

// helper to convert map[string]any to map[string]string
func stringifyValues(values map[string]any) map[string]string {
	result := make(map[string]string, len(values))
	for k, v := range values {
		switch val := v.(type) {
		case string:
			result[k] = val
		default:
			result[k] = fmt.Sprintf("%v", v)
		}
	}
	return result
}
