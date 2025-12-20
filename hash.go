package redis_ipc

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// HashPublisher provides atomic HSET + PUBLISH with change detection.
// This matches the LibreScoot pattern where:
// - State is stored in a Redis hash
// - Changes are notified via pub/sub with the field name as payload
// - Consumers fetch the value using HGET
type HashPublisher struct {
	client   *Client
	hash     string
	channel  string
	previous map[string]string
	mu       sync.Mutex
}

// NewHashPublisher creates a publisher for a hash.
// By convention, the channel name equals the hash name.
func (c *Client) NewHashPublisher(hash string) *HashPublisher {
	return &HashPublisher{
		client:   c,
		hash:     hash,
		channel:  hash,
		previous: make(map[string]string),
	}
}

// NewHashPublisherWithChannel creates a publisher with a custom channel name.
func (c *Client) NewHashPublisherWithChannel(hash, channel string) *HashPublisher {
	return &HashPublisher{
		client:   c,
		hash:     hash,
		channel:  channel,
		previous: make(map[string]string),
	}
}

// Set atomically sets a hash field and publishes the field name.
// Always publishes regardless of whether the value changed.
func (hp *HashPublisher) Set(ctx context.Context, field string, value any) error {
	strVal := stringify(value)

	pipe := hp.client.redis.Pipeline()
	pipe.HSet(ctx, hp.hash, field, strVal)
	pipe.Publish(ctx, hp.channel, field)
	_, err := pipe.Exec(ctx)

	if err == nil {
		hp.mu.Lock()
		hp.previous[field] = strVal
		hp.mu.Unlock()
	}

	return err
}

// SetIfChanged atomically sets a hash field and publishes only if the value changed.
// Returns true if the value was changed and published.
func (hp *HashPublisher) SetIfChanged(ctx context.Context, field string, value any) (bool, error) {
	strVal := stringify(value)

	hp.mu.Lock()
	prev, hasPrev := hp.previous[field]
	hp.mu.Unlock()

	if hasPrev && prev == strVal {
		return false, nil
	}

	pipe := hp.client.redis.Pipeline()
	pipe.HSet(ctx, hp.hash, field, strVal)
	pipe.Publish(ctx, hp.channel, field)
	_, err := pipe.Exec(ctx)

	if err == nil {
		hp.mu.Lock()
		hp.previous[field] = strVal
		hp.mu.Unlock()
	}

	return err == nil, err
}

// SetMany atomically sets multiple fields and publishes each one.
func (hp *HashPublisher) SetMany(ctx context.Context, fields map[string]any) error {
	if len(fields) == 0 {
		return nil
	}

	pipe := hp.client.redis.Pipeline()
	for field, value := range fields {
		strVal := stringify(value)
		pipe.HSet(ctx, hp.hash, field, strVal)
		pipe.Publish(ctx, hp.channel, field)
	}
	_, err := pipe.Exec(ctx)

	if err == nil {
		hp.mu.Lock()
		for field, value := range fields {
			hp.previous[field] = stringify(value)
		}
		hp.mu.Unlock()
	}

	return err
}

// SetManyIfChanged atomically sets multiple fields, publishing only changed ones.
// Returns the list of fields that were actually changed.
func (hp *HashPublisher) SetManyIfChanged(ctx context.Context, fields map[string]any) ([]string, error) {
	if len(fields) == 0 {
		return nil, nil
	}

	hp.mu.Lock()
	changed := make(map[string]string)
	for field, value := range fields {
		strVal := stringify(value)
		if prev, ok := hp.previous[field]; !ok || prev != strVal {
			changed[field] = strVal
		}
	}
	hp.mu.Unlock()

	if len(changed) == 0 {
		return nil, nil
	}

	pipe := hp.client.redis.Pipeline()

	// Set all fields (even unchanged, for consistency)
	for field, value := range fields {
		pipe.HSet(ctx, hp.hash, field, stringify(value))
	}

	// Only publish changed fields
	for field := range changed {
		pipe.Publish(ctx, hp.channel, field)
	}

	_, err := pipe.Exec(ctx)

	if err == nil {
		hp.mu.Lock()
		for field, value := range fields {
			hp.previous[field] = stringify(value)
		}
		hp.mu.Unlock()
	}

	changedFields := make([]string, 0, len(changed))
	for field := range changed {
		changedFields = append(changedFields, field)
	}

	return changedFields, err
}

// SetWithTimestamp atomically sets a field, its timestamp, and publishes.
func (hp *HashPublisher) SetWithTimestamp(ctx context.Context, field string, value any) error {
	strVal := stringify(value)
	tsField := field + ":timestamp"
	ts := fmt.Sprintf("%d", time.Now().UnixMilli())

	pipe := hp.client.redis.Pipeline()
	pipe.HSet(ctx, hp.hash, field, strVal)
	pipe.HSet(ctx, hp.hash, tsField, ts)
	pipe.Publish(ctx, hp.channel, field)
	_, err := pipe.Exec(ctx)

	if err == nil {
		hp.mu.Lock()
		hp.previous[field] = strVal
		hp.previous[tsField] = ts
		hp.mu.Unlock()
	}

	return err
}

// Get retrieves a field value from the hash.
func (hp *HashPublisher) Get(ctx context.Context, field string) (string, error) {
	return hp.client.redis.HGet(ctx, hp.hash, field).Result()
}

// GetAll retrieves all fields from the hash.
func (hp *HashPublisher) GetAll(ctx context.Context) (map[string]string, error) {
	return hp.client.redis.HGetAll(ctx, hp.hash).Result()
}

// Hash returns the hash name.
func (hp *HashPublisher) Hash() string {
	return hp.hash
}

// Channel returns the channel name.
func (hp *HashPublisher) Channel() string {
	return hp.channel
}

// Delete removes a field from the hash and publishes notification.
func (hp *HashPublisher) Delete(ctx context.Context, field string) error {
	pipe := hp.client.redis.Pipeline()
	pipe.HDel(ctx, hp.hash, field)
	pipe.Publish(ctx, hp.channel, field)
	_, err := pipe.Exec(ctx)

	if err == nil {
		hp.mu.Lock()
		delete(hp.previous, field)
		hp.mu.Unlock()
	}

	return err
}

// Clear deletes the entire hash and publishes "cleared" notification.
func (hp *HashPublisher) Clear(ctx context.Context) error {
	pipe := hp.client.redis.Pipeline()
	pipe.Del(ctx, hp.hash)
	pipe.Publish(ctx, hp.channel, "cleared")
	_, err := pipe.Exec(ctx)

	if err == nil {
		hp.mu.Lock()
		hp.previous = make(map[string]string)
		hp.mu.Unlock()
	}

	return err
}

// ReplaceAll atomically deletes all fields and sets new ones.
// This solves the race condition in DEL + HMSET + PUBLISH patterns.
// If fields is nil or empty, just deletes and publishes "cleared".
func (hp *HashPublisher) ReplaceAll(ctx context.Context, fields map[string]any) error {
	pipe := hp.client.redis.Pipeline()
	pipe.Del(ctx, hp.hash)

	if len(fields) > 0 {
		for field, value := range fields {
			pipe.HSet(ctx, hp.hash, field, stringify(value))
		}
		pipe.Publish(ctx, hp.channel, "replaced")
	} else {
		pipe.Publish(ctx, hp.channel, "cleared")
	}

	_, err := pipe.Exec(ctx)

	if err == nil {
		hp.mu.Lock()
		hp.previous = make(map[string]string)
		for field, value := range fields {
			hp.previous[field] = stringify(value)
		}
		hp.mu.Unlock()
	}

	return err
}

// HashWatcher subscribes to a channel and fetches hash values on notification.
// This matches the LibreScoot consumer pattern.
type HashWatcher struct {
	client   *Client
	hash     string
	channel  string
	handlers map[string]func(value string) error
	catchAll func(field, value string) error
	mu       sync.RWMutex
	pubsub   *redis.PubSub

	// Debounce support
	debounce       time.Duration
	debounceTimers map[string]*time.Timer
	debounceValues map[string]string
	debounceMu     sync.Mutex
}

// NewHashWatcher creates a watcher for a hash.
// By convention, the channel name equals the hash name.
func (c *Client) NewHashWatcher(hash string) *HashWatcher {
	return &HashWatcher{
		client:         c,
		hash:           hash,
		channel:        hash,
		handlers:       make(map[string]func(value string) error),
		debounceTimers: make(map[string]*time.Timer),
		debounceValues: make(map[string]string),
	}
}

// NewHashWatcherWithChannel creates a watcher with a custom channel name.
func (c *Client) NewHashWatcherWithChannel(hash, channel string) *HashWatcher {
	return &HashWatcher{
		client:         c,
		hash:           hash,
		channel:        channel,
		handlers:       make(map[string]func(value string) error),
		debounceTimers: make(map[string]*time.Timer),
		debounceValues: make(map[string]string),
	}
}

// SetDebounce sets a debounce duration for handler calls.
// When set, rapid updates to the same field are coalesced - only the last
// value is passed to the handler after the quiet period expires.
func (hw *HashWatcher) SetDebounce(d time.Duration) *HashWatcher {
	hw.debounce = d
	return hw
}

// OnField registers a handler for a specific field.
// When a message is received with this field name, the handler is called
// with the fetched value from the hash.
func (hw *HashWatcher) OnField(field string, handler func(value string) error) *HashWatcher {
	hw.mu.Lock()
	hw.handlers[field] = handler
	hw.mu.Unlock()
	return hw
}

// OnFieldTyped registers a typed handler for a specific field.
// The value is JSON-decoded into the type T.
func OnFieldTyped[T any](hw *HashWatcher, field string, handler func(T) error) *HashWatcher {
	hw.mu.Lock()
	hw.handlers[field] = func(value string) error {
		var val T
		if err := json.Unmarshal([]byte(value), &val); err != nil {
			// Try direct string assignment for string types
			if reflect.TypeOf((*T)(nil)).Elem().Kind() == reflect.String {
				var s any = value
				return handler(s.(T))
			}
			return fmt.Errorf("decode %s: %w", field, err)
		}
		return handler(val)
	}
	hw.mu.Unlock()
	return hw
}

// OnAny registers a catch-all handler for any field not explicitly handled.
func (hw *HashWatcher) OnAny(handler func(field, value string) error) *HashWatcher {
	hw.mu.Lock()
	hw.catchAll = handler
	hw.mu.Unlock()
	return hw
}

// Start begins listening for messages and fetching values.
func (hw *HashWatcher) Start() error {
	hw.pubsub = hw.client.redis.Subscribe(hw.client.ctx, hw.channel)

	subscribed := make(chan struct{})
	hw.client.wg.Add(1)
	go func() {
		defer hw.client.wg.Done()
		ch := hw.pubsub.Channel()
		close(subscribed)

		for msg := range ch {
			hw.processField(msg.Payload)
		}
	}()

	select {
	case <-subscribed:
		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("subscription timeout for channel %s", hw.channel)
	}
}

// StartWithSync subscribes, fetches current state, calls handlers, then processes messages.
// This ensures no messages are missed between initial fetch and subscribe.
// Order: Subscribe (messages buffer) → HGETALL → call handlers → process buffered messages
func (hw *HashWatcher) StartWithSync(ctx context.Context) error {
	// Subscribe first - messages will buffer in the channel
	hw.pubsub = hw.client.redis.Subscribe(hw.client.ctx, hw.channel)

	subscribed := make(chan struct{})
	hw.client.wg.Add(1)
	go func() {
		defer hw.client.wg.Done()
		ch := hw.pubsub.Channel()
		close(subscribed)

		// Process messages (including any buffered during initial sync)
		for msg := range ch {
			hw.processField(msg.Payload)
		}
	}()

	// Wait for subscription to be ready
	select {
	case <-subscribed:
	case <-time.After(5 * time.Second):
		return fmt.Errorf("subscription timeout for channel %s", hw.channel)
	}

	// Fetch current state and call handlers
	values, err := hw.client.redis.HGetAll(ctx, hw.hash).Result()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("initial fetch failed: %w", err)
	}

	hw.mu.RLock()
	handlers := hw.handlers
	catchAll := hw.catchAll
	hw.mu.RUnlock()

	for field, value := range values {
		if handler, ok := handlers[field]; ok {
			if err := handler(value); err != nil {
				hw.client.opts.logger.Error("initial sync handler error", "hash", hw.hash, "field", field, "error", err)
			}
		} else if catchAll != nil {
			if err := catchAll(field, value); err != nil {
				hw.client.opts.logger.Error("initial sync catch-all error", "hash", hw.hash, "field", field, "error", err)
			}
		}
	}

	return nil
}

// processField fetches and dispatches a field update, with optional debouncing.
func (hw *HashWatcher) processField(field string) {
	// Fetch the value from hash
	value, err := hw.client.redis.HGet(hw.client.ctx, hw.hash, field).Result()
	if err != nil {
		if err != redis.Nil {
			hw.client.opts.logger.Error("HGET error", "hash", hw.hash, "field", field, "error", err)
		}
		return
	}

	if hw.debounce > 0 {
		hw.processFieldDebounced(field, value)
	} else {
		hw.dispatchField(field, value)
	}
}

// processFieldDebounced handles debounced field updates.
func (hw *HashWatcher) processFieldDebounced(field, value string) {
	hw.debounceMu.Lock()
	defer hw.debounceMu.Unlock()

	// Store the latest value
	hw.debounceValues[field] = value

	// Cancel existing timer if any
	if timer, ok := hw.debounceTimers[field]; ok {
		timer.Stop()
	}

	// Create new timer
	hw.debounceTimers[field] = time.AfterFunc(hw.debounce, func() {
		hw.debounceMu.Lock()
		v := hw.debounceValues[field]
		delete(hw.debounceTimers, field)
		delete(hw.debounceValues, field)
		hw.debounceMu.Unlock()

		hw.dispatchField(field, v)
	})
}

// dispatchField calls the appropriate handler for a field.
func (hw *HashWatcher) dispatchField(field, value string) {
	hw.mu.RLock()
	handler, ok := hw.handlers[field]
	catchAll := hw.catchAll
	hw.mu.RUnlock()

	if ok {
		if err := handler(value); err != nil {
			hw.client.opts.logger.Error("handler error", "hash", hw.hash, "field", field, "error", err)
		}
	} else if catchAll != nil {
		if err := catchAll(field, value); err != nil {
			hw.client.opts.logger.Error("catch-all handler error", "hash", hw.hash, "field", field, "error", err)
		}
	}
}

// Stop stops the watcher.
func (hw *HashWatcher) Stop() error {
	if hw.pubsub != nil {
		return hw.pubsub.Close()
	}
	return nil
}

// FetchAll retrieves all current values from the hash.
// Useful for initial state sync.
func (hw *HashWatcher) FetchAll(ctx context.Context) (map[string]string, error) {
	return hw.client.redis.HGetAll(ctx, hw.hash).Result()
}

// Fetch retrieves a specific field from the hash.
func (hw *HashWatcher) Fetch(ctx context.Context, field string) (string, error) {
	return hw.client.redis.HGet(ctx, hw.hash, field).Result()
}

// FaultSet manages a Redis set of fault codes with pub/sub notification.
type FaultSet struct {
	client  *Client
	setKey  string
	channel string
	payload string
}

// NewFaultSet creates a fault set manager.
// Example: NewFaultSet("battery:0:fault", "battery:0", "fault")
func (c *Client) NewFaultSet(setKey, channel, payload string) *FaultSet {
	return &FaultSet{
		client:  c,
		setKey:  setKey,
		channel: channel,
		payload: payload,
	}
}

// Add adds a fault code to the set and publishes notification.
func (fs *FaultSet) Add(ctx context.Context, code int) error {
	pipe := fs.client.redis.Pipeline()
	pipe.SAdd(ctx, fs.setKey, code)
	pipe.Publish(ctx, fs.channel, fs.payload)
	_, err := pipe.Exec(ctx)
	return err
}

// Remove removes a fault code from the set and publishes notification.
func (fs *FaultSet) Remove(ctx context.Context, code int) error {
	pipe := fs.client.redis.Pipeline()
	pipe.SRem(ctx, fs.setKey, code)
	pipe.Publish(ctx, fs.channel, fs.payload)
	_, err := pipe.Exec(ctx)
	return err
}

// Has checks if a fault code is present.
func (fs *FaultSet) Has(ctx context.Context, code int) (bool, error) {
	return fs.client.redis.SIsMember(ctx, fs.setKey, code).Result()
}

// All returns all fault codes in the set.
func (fs *FaultSet) All(ctx context.Context) ([]string, error) {
	return fs.client.redis.SMembers(ctx, fs.setKey).Result()
}

// Clear removes all fault codes and publishes notification.
func (fs *FaultSet) Clear(ctx context.Context) error {
	pipe := fs.client.redis.Pipeline()
	pipe.Del(ctx, fs.setKey)
	pipe.Publish(ctx, fs.channel, fs.payload)
	_, err := pipe.Exec(ctx)
	return err
}

// SetMany atomically adds/removes multiple codes.
// positive codes are added, negative codes are removed.
func (fs *FaultSet) SetMany(ctx context.Context, codes []int) error {
	if len(codes) == 0 {
		return nil
	}

	pipe := fs.client.redis.Pipeline()
	for _, code := range codes {
		if code > 0 {
			pipe.SAdd(ctx, fs.setKey, code)
		} else if code < 0 {
			pipe.SRem(ctx, fs.setKey, -code)
		}
	}
	pipe.Publish(ctx, fs.channel, fs.payload)
	_, err := pipe.Exec(ctx)
	return err
}

// helper to convert any value to string
func stringify(v any) string {
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	case fmt.Stringer:
		return val.String()
	default:
		return fmt.Sprintf("%v", v)
	}
}
