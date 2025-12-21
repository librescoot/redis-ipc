package redis_ipc

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	client, err := New(WithAddress("localhost"), WithPort(6379))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	if !client.Connected() {
		t.Error("Expected client to be connected")
	}
}

func TestNewWithCallbacks(t *testing.T) {
	connectCalled := false
	client, err := New(
		WithAddress("localhost"),
		WithOnConnect(func() { connectCalled = true }),
	)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	if !connectCalled {
		t.Error("Expected onConnect callback to be called")
	}
}

func TestGetSet(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	key := "test:getset:" + time.Now().Format(time.RFC3339Nano)

	// Set
	err = client.Set(key, "hello", 10*time.Second)
	if err != nil {
		t.Fatalf("Set() failed: %v", err)
	}

	// Get
	val, err := client.Get(key)
	if err != nil {
		t.Fatalf("Get() failed: %v", err)
	}
	if val != "hello" {
		t.Errorf("Get() = %q, want %q", val, "hello")
	}

	// Cleanup
	client.Del(key)
}

func TestHGetHSet(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	key := "test:hash:" + time.Now().Format(time.RFC3339Nano)

	// HSet
	err = client.HSet(key, "field1", "value1")
	if err != nil {
		t.Fatalf("HSet() failed: %v", err)
	}

	// HGet
	val, err := client.HGet(key, "field1")
	if err != nil {
		t.Fatalf("HGet() failed: %v", err)
	}
	if val != "value1" {
		t.Errorf("HGet() = %q, want %q", val, "value1")
	}

	// HGetAll
	all, err := client.HGetAll(key)
	if err != nil {
		t.Fatalf("HGetAll() failed: %v", err)
	}
	if all["field1"] != "value1" {
		t.Errorf("HGetAll()[field1] = %q, want %q", all["field1"], "value1")
	}

	// Cleanup
	client.Del(key)
}

func TestSubscribeTyped(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	channel := "test:subscribe:" + time.Now().Format(time.RFC3339Nano)

	type TestMessage struct {
		Name  string `json:"name"`
		Value int    `json:"value"`
	}

	received := make(chan TestMessage, 1)
	sub, err := Subscribe(client, channel, func(msg TestMessage) error {
		received <- msg
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe() failed: %v", err)
	}
	defer sub.Unsubscribe()

	// Give subscription time to establish
	time.Sleep(100 * time.Millisecond)

	// Publish
	err = PublishTyped(client, channel, TestMessage{Name: "test", Value: 42})
	if err != nil {
		t.Fatalf("PublishTyped() failed: %v", err)
	}

	// Wait for message
	select {
	case msg := <-received:
		if msg.Name != "test" || msg.Value != 42 {
			t.Errorf("Received %+v, want {Name:test Value:42}", msg)
		}
	case <-time.After(2 * time.Second):
		t.Error("Timeout waiting for message")
	}
}

func TestHandleRequests(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	queue := "test:queue:" + time.Now().Format(time.RFC3339Nano)

	type Command struct {
		Action string `json:"action"`
	}

	received := make(chan Command, 1)
	handler := HandleRequests(client, queue, func(cmd Command) error {
		received <- cmd
		return nil
	})
	defer handler.Stop()

	// Give handler time to start
	time.Sleep(100 * time.Millisecond)

	// Send request
	err = SendRequest(client, queue, Command{Action: "test"})
	if err != nil {
		t.Fatalf("SendRequest() failed: %v", err)
	}

	// Wait for message
	select {
	case cmd := <-received:
		if cmd.Action != "test" {
			t.Errorf("Received %+v, want {Action:test}", cmd)
		}
	case <-time.After(2 * time.Second):
		t.Error("Timeout waiting for command")
	}
}

func TestTxGroup(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	key := "test:tx:" + time.Now().Format(time.RFC3339Nano)

	tx := client.NewTxGroup()
	tx.Add("SET", key, "value1")
	tx.Add("GET", key)

	results, err := tx.Exec()
	if err != nil {
		t.Fatalf("Exec() failed: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}

	if results[1] != "value1" {
		t.Errorf("GET result = %v, want value1", results[1])
	}

	// Cleanup
	client.Del(key)
}

func TestRouter(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	channel := "test:router:" + time.Now().Format(time.RFC3339Nano)

	type StateMsg struct {
		State string `json:"state"`
	}
	type ErrorMsg struct {
		Code int `json:"code"`
	}

	stateReceived := make(chan StateMsg, 1)
	errorReceived := make(chan ErrorMsg, 1)

	router := client.NewRouter(channel)
	Handle(router, "state", func(msg StateMsg) error {
		stateReceived <- msg
		return nil
	})
	Handle(router, "error", func(msg ErrorMsg) error {
		errorReceived <- msg
		return nil
	})

	err = router.Start()
	if err != nil {
		t.Fatalf("Router.Start() failed: %v", err)
	}
	defer router.Stop()

	// Give router time to establish
	time.Sleep(100 * time.Millisecond)

	// Publish state message
	err = PublishRouted(client, channel, "state", StateMsg{State: "ready"})
	if err != nil {
		t.Fatalf("PublishRouted() failed: %v", err)
	}

	// Publish error message
	err = PublishRouted(client, channel, "error", ErrorMsg{Code: 500})
	if err != nil {
		t.Fatalf("PublishRouted() failed: %v", err)
	}

	// Wait for messages
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		select {
		case msg := <-stateReceived:
			if msg.State != "ready" {
				t.Errorf("State = %q, want ready", msg.State)
			}
		case <-time.After(2 * time.Second):
			t.Error("Timeout waiting for state message")
		}
	}()

	go func() {
		defer wg.Done()
		select {
		case msg := <-errorReceived:
			if msg.Code != 500 {
				t.Errorf("Code = %d, want 500", msg.Code)
			}
		case <-time.After(2 * time.Second):
			t.Error("Timeout waiting for error message")
		}
	}()

	wg.Wait()
}

func TestGracefulShutdown(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Start a queue handler
	queue := "test:shutdown:" + time.Now().Format(time.RFC3339Nano)
	HandleRequests(client, queue, func(msg string) error {
		return nil
	})

	// Close should wait for handlers
	start := time.Now()
	err = client.CloseWithTimeout(1 * time.Second)
	elapsed := time.Since(start)

	if err != nil {
		t.Errorf("CloseWithTimeout() failed: %v", err)
	}

	// Should complete quickly since handler isn't processing anything
	if elapsed > 500*time.Millisecond {
		t.Errorf("Close took %v, expected faster shutdown", elapsed)
	}
}

func TestWithURL(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		wantAddr string
		wantPort int
	}{
		{"full url", "redis://myhost:1234", "myhost", 1234},
		{"host:port", "myhost:1234", "myhost", 1234},
		{"just host", "myhost", "myhost", 6379},
		{"localhost url", "redis://localhost:6379", "localhost", 6379},
		{"no port", "redis://myhost", "myhost", 6379},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := defaultOptions()
			WithURL(tt.url)(o)

			if o.address != tt.wantAddr {
				t.Errorf("address = %q, want %q", o.address, tt.wantAddr)
			}
			if o.port != tt.wantPort {
				t.Errorf("port = %d, want %d", o.port, tt.wantPort)
			}
		})
	}
}

func TestWithURLConnection(t *testing.T) {
	// Test that WithURL actually connects
	client, err := New(WithURL("redis://localhost:6379"))
	if err != nil {
		t.Fatalf("New() with WithURL failed: %v", err)
	}
	defer client.Close()

	if !client.Connected() {
		t.Error("Expected client to be connected")
	}
}

func TestPing(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	err = client.Ping()
	if err != nil {
		t.Errorf("Ping() failed: %v", err)
	}
}

func TestHash(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	hash := "test:hash-cache:" + time.Now().Format(time.RFC3339Nano)

	// Get publisher twice - should be same instance
	pub1 := client.Hash(hash)
	pub2 := client.Hash(hash)

	if pub1 != pub2 {
		t.Error("Hash() should return cached instance")
	}

	// Verify it works
	err = pub1.Set("field", "value")
	if err != nil {
		t.Fatalf("Set() failed: %v", err)
	}

	val, err := pub2.Get("field")
	if err != nil {
		t.Fatalf("Get() failed: %v", err)
	}
	if val != "value" {
		t.Errorf("Get() = %q, want %q", val, "value")
	}

	// Cleanup
	client.Del(hash)
}

func TestWithContext(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	key := "test:withctx:" + time.Now().Format(time.RFC3339Nano)

	// Create a custom context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// WithContext returns a ContextClient that uses the custom context
	cc := client.WithContext(ctx)

	// Verify Context() returns the custom context
	if cc.Context() != ctx {
		t.Error("ContextClient.Context() should return custom context")
	}

	// Operations should work through ContextClient
	err = cc.Set(key, "value", 0)
	if err != nil {
		t.Fatalf("ContextClient.Set() failed: %v", err)
	}

	val, err := cc.Get(key)
	if err != nil {
		t.Fatalf("ContextClient.Get() failed: %v", err)
	}
	if val != "value" {
		t.Errorf("Get() = %q, want %q", val, "value")
	}

	// Cleanup
	client.Del(key)
}
