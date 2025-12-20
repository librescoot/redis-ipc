package redis_ipc

import (
	"context"
	"testing"
	"time"
)

func TestStreamPublisher(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	stream := "test:stream:" + time.Now().Format(time.RFC3339Nano)

	pub := client.NewStreamPublisher(stream, WithMaxLen(100))

	// Add a message
	id, err := pub.Add(map[string]any{
		"group": "test",
		"code":  "42",
		"msg":   "hello",
	})
	if err != nil {
		t.Fatalf("Add() failed: %v", err)
	}
	if id == "" {
		t.Error("Expected non-empty ID")
	}

	// Verify message exists
	msgs, err := client.redis.XRange(client.Context(), stream, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange() failed: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(msgs))
	}
	if msgs[0].Values["group"] != "test" {
		t.Errorf("group = %v, want test", msgs[0].Values["group"])
	}

	// Cleanup
	client.redis.Del(client.Context(), stream)
}

func TestStreamPublisherTyped(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	stream := "test:streamtyped:" + time.Now().Format(time.RFC3339Nano)

	type FaultEvent struct {
		Group       string `json:"group"`
		Code        int    `json:"code"`
		Description string `json:"description"`
	}

	pub := client.NewStreamPublisher(stream)

	id, err := StreamAdd(pub, FaultEvent{
		Group:       "battery:0",
		Code:        35,
		Description: "NFC Reader Error",
	})
	if err != nil {
		t.Fatalf("StreamAdd() failed: %v", err)
	}
	if id == "" {
		t.Error("Expected non-empty ID")
	}

	// Verify
	msgs, err := client.redis.XRange(client.Context(), stream, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange() failed: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(msgs))
	}
	if msgs[0].Values["group"] != "battery:0" {
		t.Errorf("group = %v, want battery:0", msgs[0].Values["group"])
	}

	// Cleanup
	client.redis.Del(client.Context(), stream)
}

func TestStreamConsumer(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	stream := "test:consumer:" + time.Now().Format(time.RFC3339Nano)

	// Create publisher
	pub := client.NewStreamPublisher(stream)

	// Create consumer
	received := make(chan map[string]string, 1)
	consumer := client.NewStreamConsumer(stream, WithBlockTimeout(100*time.Millisecond))
	consumer.Handle(func(id string, values map[string]string) error {
		received <- values
		return nil
	})

	// Start consumer from beginning
	err = consumer.Start("0")
	if err != nil {
		t.Fatalf("Start() failed: %v", err)
	}

	// Give consumer time to start
	time.Sleep(50 * time.Millisecond)

	// Publish a message
	_, err = pub.Add(map[string]any{
		"test": "value",
	})
	if err != nil {
		t.Fatalf("Add() failed: %v", err)
	}

	// Wait for message
	select {
	case values := <-received:
		if values["test"] != "value" {
			t.Errorf("test = %v, want value", values["test"])
		}
	case <-time.After(2 * time.Second):
		t.Error("Timeout waiting for message")
	}

	// Cleanup
	client.redis.Del(context.Background(), stream)
}

func TestStreamConsumerTyped(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	stream := "test:consumertyped:" + time.Now().Format(time.RFC3339Nano)

	type Event struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	}

	// Create publisher
	pub := client.NewStreamPublisher(stream)

	// Create typed consumer
	received := make(chan Event, 1)
	consumer := client.NewStreamConsumer(stream, WithBlockTimeout(100*time.Millisecond))
	StreamHandle(consumer, func(id string, evt Event) error {
		received <- evt
		return nil
	})

	err = consumer.Start("0")
	if err != nil {
		t.Fatalf("Start() failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// Publish
	_, err = StreamAdd(pub, Event{Name: "test", Value: "123"})
	if err != nil {
		t.Fatalf("StreamAdd() failed: %v", err)
	}

	// Wait for message
	select {
	case evt := <-received:
		if evt.Name != "test" || evt.Value != "123" {
			t.Errorf("Received %+v, want {Name:test Value:123}", evt)
		}
	case <-time.After(2 * time.Second):
		t.Error("Timeout waiting for message")
	}

	// Cleanup
	client.redis.Del(context.Background(), stream)
}

func TestStreamConsumerGroup(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	stream := "test:consumergroup:" + time.Now().Format(time.RFC3339Nano)
	group := "test-group"
	consumerName := "consumer-1"

	// Create publisher
	pub := client.NewStreamPublisher(stream)

	// Create consumer with group
	received := make(chan string, 1)
	consumer := client.NewStreamConsumer(stream,
		WithBlockTimeout(100*time.Millisecond),
		WithConsumerGroup(group, consumerName),
	)
	consumer.Handle(func(id string, values map[string]string) error {
		received <- values["msg"]
		return nil
	})

	err = consumer.Start(">")
	if err != nil {
		t.Fatalf("Start() failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// Publish
	_, err = pub.Add(map[string]any{"msg": "hello"})
	if err != nil {
		t.Fatalf("Add() failed: %v", err)
	}

	// Wait for message
	select {
	case msg := <-received:
		if msg != "hello" {
			t.Errorf("msg = %v, want hello", msg)
		}
	case <-time.After(2 * time.Second):
		t.Error("Timeout waiting for message")
	}

	// Cleanup
	client.redis.XGroupDestroy(context.Background(), stream, group)
	client.redis.Del(context.Background(), stream)
}

func TestStreamPublisherMaxLen(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	stream := "test:maxlen:" + time.Now().Format(time.RFC3339Nano)

	pub := client.NewStreamPublisher(stream, WithMaxLen(5))

	// Add 10 messages
	for i := 0; i < 10; i++ {
		_, err := pub.Add(map[string]any{"i": i})
		if err != nil {
			t.Fatalf("Add() failed: %v", err)
		}
	}

	// Check length (should be approximately 5 due to MAXLEN ~)
	length, err := client.redis.XLen(client.Context(), stream).Result()
	if err != nil {
		t.Fatalf("XLen() failed: %v", err)
	}
	// With approximate MAXLEN, it might be slightly more than 5
	if length > 10 {
		t.Errorf("Stream length = %d, expected <= 10", length)
	}

	// Cleanup
	client.redis.Del(client.Context(), stream)
}
