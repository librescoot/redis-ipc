package redis_ipc

import (
	"context"
	"testing"
	"time"
)

func TestHashPublisher(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	hash := "test:hashpub:" + time.Now().Format(time.RFC3339Nano)

	pub := client.NewHashPublisher(hash)

	// Set a field
	err = pub.Set(ctx, "state", "ready")
	if err != nil {
		t.Fatalf("Set() failed: %v", err)
	}

	// Verify it was set
	val, err := pub.Get(ctx, "state")
	if err != nil {
		t.Fatalf("Get() failed: %v", err)
	}
	if val != "ready" {
		t.Errorf("Get() = %q, want ready", val)
	}

	// Cleanup
	client.Del(ctx, hash)
}

func TestHashPublisherSetIfChanged(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	hash := "test:hashchange:" + time.Now().Format(time.RFC3339Nano)

	pub := client.NewHashPublisher(hash)

	// First set should change
	changed, err := pub.SetIfChanged(ctx, "state", "ready")
	if err != nil {
		t.Fatalf("SetIfChanged() failed: %v", err)
	}
	if !changed {
		t.Error("First SetIfChanged should return true")
	}

	// Second set with same value should not change
	changed, err = pub.SetIfChanged(ctx, "state", "ready")
	if err != nil {
		t.Fatalf("SetIfChanged() failed: %v", err)
	}
	if changed {
		t.Error("SetIfChanged with same value should return false")
	}

	// Third set with different value should change
	changed, err = pub.SetIfChanged(ctx, "state", "parked")
	if err != nil {
		t.Fatalf("SetIfChanged() failed: %v", err)
	}
	if !changed {
		t.Error("SetIfChanged with different value should return true")
	}

	// Cleanup
	client.Del(ctx, hash)
}

func TestHashPublisherSetMany(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	hash := "test:hashmany:" + time.Now().Format(time.RFC3339Nano)

	pub := client.NewHashPublisher(hash)

	fields := map[string]any{
		"state":   "active",
		"charge":  "85",
		"voltage": "52000",
	}

	err = pub.SetMany(ctx, fields)
	if err != nil {
		t.Fatalf("SetMany() failed: %v", err)
	}

	// Verify all fields
	all, err := pub.GetAll(ctx)
	if err != nil {
		t.Fatalf("GetAll() failed: %v", err)
	}

	if all["state"] != "active" {
		t.Errorf("state = %q, want active", all["state"])
	}
	if all["charge"] != "85" {
		t.Errorf("charge = %q, want 85", all["charge"])
	}
	if all["voltage"] != "52000" {
		t.Errorf("voltage = %q, want 52000", all["voltage"])
	}

	// Cleanup
	client.Del(ctx, hash)
}

func TestHashPublisherSetManyIfChanged(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	hash := "test:hashmanychange:" + time.Now().Format(time.RFC3339Nano)

	pub := client.NewHashPublisher(hash)

	// Initial set
	fields := map[string]any{
		"state":  "active",
		"charge": "85",
	}
	changed, err := pub.SetManyIfChanged(ctx, fields)
	if err != nil {
		t.Fatalf("SetManyIfChanged() failed: %v", err)
	}
	if len(changed) != 2 {
		t.Errorf("Expected 2 changed fields, got %d", len(changed))
	}

	// Update with one changed field
	fields2 := map[string]any{
		"state":  "active", // same
		"charge": "80",     // different
	}
	changed, err = pub.SetManyIfChanged(ctx, fields2)
	if err != nil {
		t.Fatalf("SetManyIfChanged() failed: %v", err)
	}
	if len(changed) != 1 {
		t.Errorf("Expected 1 changed field, got %d: %v", len(changed), changed)
	}
	if len(changed) == 1 && changed[0] != "charge" {
		t.Errorf("Changed field = %q, want charge", changed[0])
	}

	// Cleanup
	client.Del(ctx, hash)
}

func TestHashWatcher(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	hash := "test:hashwatch:" + time.Now().Format(time.RFC3339Nano)

	// Set up watcher
	received := make(chan string, 1)
	watcher := client.NewHashWatcher(hash)
	watcher.OnField("state", func(value string) error {
		received <- value
		return nil
	})

	err = watcher.Start()
	if err != nil {
		t.Fatalf("Start() failed: %v", err)
	}
	defer watcher.Stop()

	// Give watcher time to subscribe
	time.Sleep(100 * time.Millisecond)

	// Publish using HashPublisher
	pub := client.NewHashPublisher(hash)
	err = pub.Set(ctx, "state", "ready")
	if err != nil {
		t.Fatalf("Set() failed: %v", err)
	}

	// Wait for notification
	select {
	case val := <-received:
		if val != "ready" {
			t.Errorf("Received %q, want ready", val)
		}
	case <-time.After(2 * time.Second):
		t.Error("Timeout waiting for field notification")
	}

	// Cleanup
	client.Del(ctx, hash)
}

func TestHashWatcherCatchAll(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	hash := "test:hashcatchall:" + time.Now().Format(time.RFC3339Nano)

	type fieldValue struct {
		field string
		value string
	}
	received := make(chan fieldValue, 1)

	watcher := client.NewHashWatcher(hash)
	watcher.OnAny(func(field, value string) error {
		received <- fieldValue{field, value}
		return nil
	})

	err = watcher.Start()
	if err != nil {
		t.Fatalf("Start() failed: %v", err)
	}
	defer watcher.Stop()

	// Give watcher time to subscribe
	time.Sleep(100 * time.Millisecond)

	// Publish
	pub := client.NewHashPublisher(hash)
	err = pub.Set(ctx, "unknown-field", "some-value")
	if err != nil {
		t.Fatalf("Set() failed: %v", err)
	}

	// Wait for notification
	select {
	case fv := <-received:
		if fv.field != "unknown-field" || fv.value != "some-value" {
			t.Errorf("Received %+v, want {unknown-field some-value}", fv)
		}
	case <-time.After(2 * time.Second):
		t.Error("Timeout waiting for catch-all notification")
	}

	// Cleanup
	client.Del(ctx, hash)
}

func TestFaultSet(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	setKey := "test:faultset:" + time.Now().Format(time.RFC3339Nano)
	channel := "test:faultchan:" + time.Now().Format(time.RFC3339Nano)

	faults := client.NewFaultSet(setKey, channel, "fault")

	// Add fault
	err = faults.Add(ctx, 35)
	if err != nil {
		t.Fatalf("Add() failed: %v", err)
	}

	// Check if present
	has, err := faults.Has(ctx, 35)
	if err != nil {
		t.Fatalf("Has() failed: %v", err)
	}
	if !has {
		t.Error("Expected fault 35 to be present")
	}

	// Get all
	all, err := faults.All(ctx)
	if err != nil {
		t.Fatalf("All() failed: %v", err)
	}
	if len(all) != 1 || all[0] != "35" {
		t.Errorf("All() = %v, want [35]", all)
	}

	// Remove fault
	err = faults.Remove(ctx, 35)
	if err != nil {
		t.Fatalf("Remove() failed: %v", err)
	}

	// Check if absent
	has, err = faults.Has(ctx, 35)
	if err != nil {
		t.Fatalf("Has() failed: %v", err)
	}
	if has {
		t.Error("Expected fault 35 to be absent")
	}

	// Cleanup
	client.Del(ctx, setKey)
}

func TestFaultSetMany(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	setKey := "test:faultsetmany:" + time.Now().Format(time.RFC3339Nano)
	channel := "test:faultchanmany:" + time.Now().Format(time.RFC3339Nano)

	faults := client.NewFaultSet(setKey, channel, "fault")

	// Add multiple faults (positive = add, negative = remove)
	err = faults.SetMany(ctx, []int{10, 20, 30})
	if err != nil {
		t.Fatalf("SetMany() failed: %v", err)
	}

	all, _ := faults.All(ctx)
	if len(all) != 3 {
		t.Errorf("Expected 3 faults, got %d", len(all))
	}

	// Remove one fault
	err = faults.SetMany(ctx, []int{-20})
	if err != nil {
		t.Fatalf("SetMany() failed: %v", err)
	}

	all, _ = faults.All(ctx)
	if len(all) != 2 {
		t.Errorf("Expected 2 faults after remove, got %d", len(all))
	}

	// Cleanup
	client.Del(ctx, setKey)
}

func TestHashPublisherSetWithTimestamp(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	hash := "test:hashts:" + time.Now().Format(time.RFC3339Nano)

	pub := client.NewHashPublisher(hash)

	// Set with timestamp
	err = pub.SetWithTimestamp(ctx, "state", "ready")
	if err != nil {
		t.Fatalf("SetWithTimestamp() failed: %v", err)
	}

	// Verify both fields
	all, err := pub.GetAll(ctx)
	if err != nil {
		t.Fatalf("GetAll() failed: %v", err)
	}

	if all["state"] != "ready" {
		t.Errorf("state = %q, want ready", all["state"])
	}
	if _, ok := all["state:timestamp"]; !ok {
		t.Error("Expected state:timestamp field to exist")
	}

	// Cleanup
	client.Del(ctx, hash)
}
