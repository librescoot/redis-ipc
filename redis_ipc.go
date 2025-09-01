package redis_ipc

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type Config struct {
	Address       string
	Port          int
	RetryInterval time.Duration
	MaxRetries    int
}

type Client struct {
	cfg         Config
	redis       *redis.Client
	ctx         context.Context
	cancel      context.CancelFunc
	subGroups   sync.Map
	reqHandlers sync.Map
}

func New(cfg Config) (*Client, error) {
	ctx, cancel := context.WithCancel(context.Background())

	opts := &redis.Options{
		Addr:        fmt.Sprintf("%s:%d", cfg.Address, cfg.Port),
		DialTimeout: 2 * time.Second,
		PoolSize:    3,
		MaxRetries:  cfg.MaxRetries,
	}

	redis := redis.NewClient(opts)

	c := &Client{
		cfg:    cfg,
		redis:  redis,
		ctx:    ctx,
		cancel: cancel,
	}

	if err := c.redis.Ping(ctx).Err(); err != nil {
		cancel()
		return nil, fmt.Errorf("redis connect failed: %w", err)
	}

	go c.monitorConnection()
	return c, nil
}

func (c *Client) monitorConnection() {
	ticker := time.NewTicker(c.cfg.RetryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			if err := c.redis.Ping(context.Background()).Err(); err != nil {
				log.Printf("Connection check failed: %v", err)
			}
		}
	}
}

type SubscriberGroup struct {
	name     string
	client   *Client
	handlers map[string]func([]byte) error // init-only, no mutex
}

func (c *Client) Subscribe(name string) *SubscriberGroup {
	if sg, ok := c.subGroups.Load(name); ok {
		return sg.(*SubscriberGroup)
	}

	sg := &SubscriberGroup{
		name:     name,
		client:   c,
		handlers: make(map[string]func([]byte) error),
	}
	c.subGroups.Store(name, sg)
	return sg
}

func (sg *SubscriberGroup) Handle(channel string, handler func([]byte) error) error {
	sg.handlers[channel] = handler

	pubsub := sg.client.redis.Subscribe(sg.client.ctx, channel)

	subscribed := make(chan struct{})
	go func() {
		ch := pubsub.Channel()
		close(subscribed)
		for msg := range ch {
			if err := handler([]byte(msg.Payload)); err != nil {
				log.Printf("Handler error: %v", err)
			}
		}
	}()

	select {
	case <-subscribed:
		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("subscription timeout")
	}
}

type RequestHandler struct {
	name    string
	client  *Client
	handler func([]byte) error
}

func (c *Client) HandleRequests(name string, handler func([]byte) error) *RequestHandler {
	if rh, ok := c.reqHandlers.Load(name); ok {
		return rh.(*RequestHandler)
	}

	rh := &RequestHandler{
		name:    name,
		client:  c,
		handler: handler,
	}
	c.reqHandlers.Store(name, rh)
	go rh.processLoopWithRestart()
	return rh
}

func (rh *RequestHandler) processLoopWithRestart() {
	for {
		if rh.client.ctx.Err() != nil {
			log.Printf("Request handler for '%s' shutting down due to client context cancellation", rh.name)
			return
		}

		log.Printf("Starting request handler loop for '%s'", rh.name)
		rh.processLoop()

		if rh.client.ctx.Err() != nil {
			return
		}

		log.Printf("Request handler loop for '%s' exited, restarting in 5 seconds...", rh.name)
		time.Sleep(5 * time.Second)
	}
}

func (rh *RequestHandler) processLoop() {
	for {
		result, err := rh.client.redis.BRPop(rh.client.ctx, 1*time.Second, rh.name).Result()
		if err == redis.Nil {
			continue
		}
		if err != nil {
			log.Printf("BRPOP error for '%s': %v", rh.name, err)
			return
		}

		if err := rh.handler([]byte(result[1])); err != nil {
			log.Printf("Handler error for '%s': %v", rh.name, err)
		}
	}
}

type TxGroup struct {
	name   string
	client *Client
	pipe   redis.Pipeliner // thread-safe internally
}

func (c *Client) NewTxGroup(name string) *TxGroup {
	return &TxGroup{
		name:   name,
		client: c,
		pipe:   c.redis.Pipeline(),
	}
}

func (g *TxGroup) Add(cmd string, args ...interface{}) error {
	cmdArgs := append([]interface{}{cmd}, args...)
	return g.pipe.Do(g.client.ctx, cmdArgs...).Err()
}

func (g *TxGroup) Exec() ([]interface{}, error) {
	// Execute pipeline and get results
	cmders, err := g.pipe.Exec(g.client.ctx)
	if err != nil && err != redis.Nil {
		return nil, err
	}

	// Convert cmders to a more usable format
	results := make([]interface{}, len(cmders))
	for i, cmder := range cmders {
		val, err := cmder.(*redis.Cmd).Result()
		if err != nil {
			// Pass through redis.Nil and other errors in the result
			results[i] = err
			continue
		}
		results[i] = val
	}

	// Reset pipeline after execution
	g.pipe = g.client.redis.Pipeline()
	return results, nil
}

// Do executes a Redis command and returns the result
func (c *Client) Do(cmd string, args ...interface{}) (interface{}, error) {
	cmdArgs := append([]interface{}{cmd}, args...)
	return c.redis.Do(c.ctx, cmdArgs...).Result()
}

// Get retrieves the value of a key
func (c *Client) Get(key string) (string, error) {
	return c.redis.Get(c.ctx, key).Result()
}

// Set sets the value of a key
func (c *Client) Set(key string, value interface{}, expiration time.Duration) error {
	return c.redis.Set(c.ctx, key, value, expiration).Err()
}

// HGet retrieves the value of a hash field
func (c *Client) HGet(key, field string) (string, error) {
	return c.redis.HGet(c.ctx, key, field).Result()
}

// HSet sets the value of a hash field
func (c *Client) HSet(key, field string, value interface{}) error {
	return c.redis.HSet(c.ctx, key, field, value).Err()
}

// HGetAll retrieves all fields and values of a hash
func (c *Client) HGetAll(key string) (map[string]string, error) {
	return c.redis.HGetAll(c.ctx, key).Result()
}

// LPush inserts values at the head of a list
func (c *Client) LPush(key string, values ...interface{}) (int64, error) {
	return c.redis.LPush(c.ctx, key, values...).Result()
}

// RPush inserts values at the tail of a list
func (c *Client) RPush(key string, values ...interface{}) (int64, error) {
	return c.redis.RPush(c.ctx, key, values...).Result()
}

// LPop removes and returns the first element of a list
func (c *Client) LPop(key string) (string, error) {
	return c.redis.LPop(c.ctx, key).Result()
}

// RPop removes and returns the last element of a list
func (c *Client) RPop(key string) (string, error) {
	return c.redis.RPop(c.ctx, key).Result()
}

// BLPop blocks until it can remove and return the first element of a list
func (c *Client) BLPop(timeout time.Duration, keys ...string) ([]string, error) {
	return c.redis.BLPop(c.ctx, timeout, keys...).Result()
}

// BRPop blocks until it can remove and return the last element of a list
func (c *Client) BRPop(timeout time.Duration, keys ...string) ([]string, error) {
	return c.redis.BRPop(c.ctx, timeout, keys...).Result()
}

// Publish publishes a message to a channel
func (c *Client) Publish(channel string, message interface{}) (int64, error) {
	return c.redis.Publish(c.ctx, channel, message).Result()
}

// Incr increments the integer value of a key by one
func (c *Client) Incr(key string) (int64, error) {
	return c.redis.Incr(c.ctx, key).Result()
}

// Decr decrements the integer value of a key by one
func (c *Client) Decr(key string) (int64, error) {
	return c.redis.Decr(c.ctx, key).Result()
}

// Exists checks if a key exists
func (c *Client) Exists(keys ...string) (int64, error) {
	return c.redis.Exists(c.ctx, keys...).Result()
}

// Del deletes a key
func (c *Client) Del(keys ...string) (int64, error) {
	return c.redis.Del(c.ctx, keys...).Result()
}

// Expire sets a key's time to live in seconds
func (c *Client) Expire(key string, expiration time.Duration) (bool, error) {
	return c.redis.Expire(c.ctx, key, expiration).Result()
}

func (c *Client) Close() error {
	c.cancel()
	return c.redis.Close()
}
