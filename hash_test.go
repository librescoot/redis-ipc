package redis_ipc

import (
	"testing"
	"time"
)

func TestHashPublisher(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	hash := "test:hashpub:" + time.Now().Format(time.RFC3339Nano)

	pub := client.NewHashPublisher(hash)

	// Set a field
	err = pub.Set("state", "ready")
	if err != nil {
		t.Fatalf("Set() failed: %v", err)
	}

	// Verify it was set
	val, err := pub.Get("state")
	if err != nil {
		t.Fatalf("Get() failed: %v", err)
	}
	if val != "ready" {
		t.Errorf("Get() = %q, want ready", val)
	}

	// Cleanup
	client.Del(hash)
}

func TestHashPublisherSetIfChanged(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	hash := "test:hashchange:" + time.Now().Format(time.RFC3339Nano)

	pub := client.NewHashPublisher(hash)

	// First set should change
	changed, err := pub.SetIfChanged("state", "ready")
	if err != nil {
		t.Fatalf("SetIfChanged() failed: %v", err)
	}
	if !changed {
		t.Error("First SetIfChanged should return true")
	}

	// Second set with same value should not change
	changed, err = pub.SetIfChanged("state", "ready")
	if err != nil {
		t.Fatalf("SetIfChanged() failed: %v", err)
	}
	if changed {
		t.Error("SetIfChanged with same value should return false")
	}

	// Third set with different value should change
	changed, err = pub.SetIfChanged("state", "parked")
	if err != nil {
		t.Fatalf("SetIfChanged() failed: %v", err)
	}
	if !changed {
		t.Error("SetIfChanged with different value should return true")
	}

	// Cleanup
	client.Del(hash)
}

func TestHashPublisherSetMany(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	hash := "test:hashmany:" + time.Now().Format(time.RFC3339Nano)

	pub := client.NewHashPublisher(hash)

	fields := map[string]any{
		"state":   "active",
		"charge":  "85",
		"voltage": "52000",
	}

	err = pub.SetMany(fields)
	if err != nil {
		t.Fatalf("SetMany() failed: %v", err)
	}

	// Verify all fields
	all, err := pub.GetAll()
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
	client.Del(hash)
}

func TestHashPublisherSetManyIfChanged(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	hash := "test:hashmanychange:" + time.Now().Format(time.RFC3339Nano)

	pub := client.NewHashPublisher(hash)

	// Initial set
	fields := map[string]any{
		"state":  "active",
		"charge": "85",
	}
	changed, err := pub.SetManyIfChanged(fields)
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
	changed, err = pub.SetManyIfChanged(fields2)
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
	client.Del(hash)
}

func TestHashWatcher(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

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
	err = pub.Set("state", "ready")
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
	client.Del(hash)
}

func TestHashWatcherCatchAll(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

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
	err = pub.Set("unknown-field", "some-value")
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
	client.Del(hash)
}

func TestFaultSet(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	setKey := "test:faultset:" + time.Now().Format(time.RFC3339Nano)
	channel := "test:faultchan:" + time.Now().Format(time.RFC3339Nano)

	faults := client.NewFaultSet(setKey, channel, "fault")

	// Add fault
	err = faults.Add(35)
	if err != nil {
		t.Fatalf("Add() failed: %v", err)
	}

	// Check if present
	has, err := faults.Has(35)
	if err != nil {
		t.Fatalf("Has() failed: %v", err)
	}
	if !has {
		t.Error("Expected fault 35 to be present")
	}

	// Get all
	all, err := faults.All()
	if err != nil {
		t.Fatalf("All() failed: %v", err)
	}
	if len(all) != 1 || all[0] != "35" {
		t.Errorf("All() = %v, want [35]", all)
	}

	// Remove fault
	err = faults.Remove(35)
	if err != nil {
		t.Fatalf("Remove() failed: %v", err)
	}

	// Check if absent
	has, err = faults.Has(35)
	if err != nil {
		t.Fatalf("Has() failed: %v", err)
	}
	if has {
		t.Error("Expected fault 35 to be absent")
	}

	// Cleanup
	client.Del(setKey)
}

func TestFaultSetMany(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	setKey := "test:faultsetmany:" + time.Now().Format(time.RFC3339Nano)
	channel := "test:faultchanmany:" + time.Now().Format(time.RFC3339Nano)

	faults := client.NewFaultSet(setKey, channel, "fault")

	// Add multiple faults (positive = add, negative = remove)
	err = faults.SetMany([]int{10, 20, 30})
	if err != nil {
		t.Fatalf("SetMany() failed: %v", err)
	}

	all, _ := faults.All()
	if len(all) != 3 {
		t.Errorf("Expected 3 faults, got %d", len(all))
	}

	// Remove one fault
	err = faults.SetMany([]int{-20})
	if err != nil {
		t.Fatalf("SetMany() failed: %v", err)
	}

	all, _ = faults.All()
	if len(all) != 2 {
		t.Errorf("Expected 2 faults after remove, got %d", len(all))
	}

	// Cleanup
	client.Del(setKey)
}

func TestHashPublisherSetWithTimestamp(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	hash := "test:hashts:" + time.Now().Format(time.RFC3339Nano)

	pub := client.NewHashPublisher(hash)

	// Set with timestamp
	err = pub.SetWithTimestamp("state", "ready")
	if err != nil {
		t.Fatalf("SetWithTimestamp() failed: %v", err)
	}

	// Verify both fields
	all, err := pub.GetAll()
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
	client.Del(hash)
}

func TestHashWatcherStartWithSync(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	hash := "test:hashsync:" + time.Now().Format(time.RFC3339Nano)

	// Pre-populate the hash with initial values
	pub := client.NewHashPublisher(hash)
	err = pub.SetMany(map[string]any{
		"state":  "ready",
		"charge": "85",
	})
	if err != nil {
		t.Fatalf("SetMany() failed: %v", err)
	}

	// Set up watcher with handlers
	stateReceived := make(chan string, 2)
	chargeReceived := make(chan string, 2)

	watcher := client.NewHashWatcher(hash)
	watcher.OnField("state", func(value string) error {
		stateReceived <- value
		return nil
	})
	watcher.OnField("charge", func(value string) error {
		chargeReceived <- value
		return nil
	})

	// StartWithSync should call handlers with initial values
	err = watcher.StartWithSync()
	if err != nil {
		t.Fatalf("StartWithSync() failed: %v", err)
	}
	defer watcher.Stop()

	// Should receive initial values
	select {
	case val := <-stateReceived:
		if val != "ready" {
			t.Errorf("Initial state = %q, want ready", val)
		}
	case <-time.After(2 * time.Second):
		t.Error("Timeout waiting for initial state")
	}

	select {
	case val := <-chargeReceived:
		if val != "85" {
			t.Errorf("Initial charge = %q, want 85", val)
		}
	case <-time.After(2 * time.Second):
		t.Error("Timeout waiting for initial charge")
	}

	// Now publish a new value and verify we receive updates too
	err = pub.Set("state", "parked")
	if err != nil {
		t.Fatalf("Set() failed: %v", err)
	}

	select {
	case val := <-stateReceived:
		if val != "parked" {
			t.Errorf("Updated state = %q, want parked", val)
		}
	case <-time.After(2 * time.Second):
		t.Error("Timeout waiting for updated state")
	}

	// Cleanup
	client.Del(hash)
}

func TestHashWatcherDebounce(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	hash := "test:hashdebounce:" + time.Now().Format(time.RFC3339Nano)

	// Track all values received
	received := make(chan string, 10)

	watcher := client.NewHashWatcher(hash)
	watcher.SetDebounce(200 * time.Millisecond)
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

	// Publish rapid updates
	pub := client.NewHashPublisher(hash)
	for _, state := range []string{"a", "b", "c", "d", "e"} {
		err = pub.Set("state", state)
		if err != nil {
			t.Fatalf("Set() failed: %v", err)
		}
		time.Sleep(50 * time.Millisecond) // Faster than debounce
	}

	// Wait for debounce to fire
	time.Sleep(300 * time.Millisecond)

	// Should only receive the last value due to debouncing
	select {
	case val := <-received:
		if val != "e" {
			t.Errorf("Debounced value = %q, want e", val)
		}
	case <-time.After(2 * time.Second):
		t.Error("Timeout waiting for debounced value")
	}

	// Should not receive any more values
	select {
	case val := <-received:
		t.Errorf("Unexpected extra value received: %q", val)
	case <-time.After(100 * time.Millisecond):
		// Expected - no more values
	}

	// Cleanup
	client.Del(hash)
}

func TestHashPublisherDelete(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	hash := "test:hashdelete:" + time.Now().Format(time.RFC3339Nano)

	pub := client.NewHashPublisher(hash)

	// Set some fields
	err = pub.SetMany(map[string]any{
		"field1": "value1",
		"field2": "value2",
	})
	if err != nil {
		t.Fatalf("SetMany() failed: %v", err)
	}

	// Delete one field
	err = pub.Delete("field1")
	if err != nil {
		t.Fatalf("Delete() failed: %v", err)
	}

	// Verify field1 is gone but field2 remains
	all, err := pub.GetAll()
	if err != nil {
		t.Fatalf("GetAll() failed: %v", err)
	}
	if _, ok := all["field1"]; ok {
		t.Error("field1 should be deleted")
	}
	if all["field2"] != "value2" {
		t.Errorf("field2 = %q, want value2", all["field2"])
	}

	// Cleanup
	client.Del(hash)
}

func TestHashPublisherClear(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	hash := "test:hashclear:" + time.Now().Format(time.RFC3339Nano)

	pub := client.NewHashPublisher(hash)

	// Set some fields
	err = pub.SetMany(map[string]any{
		"field1": "value1",
		"field2": "value2",
	})
	if err != nil {
		t.Fatalf("SetMany() failed: %v", err)
	}

	// Clear the hash
	err = pub.Clear()
	if err != nil {
		t.Fatalf("Clear() failed: %v", err)
	}

	// Verify hash is empty
	all, err := pub.GetAll()
	if err != nil {
		t.Fatalf("GetAll() failed: %v", err)
	}
	if len(all) != 0 {
		t.Errorf("Hash should be empty, got %d fields", len(all))
	}

	// Cleanup
	client.Del(hash)
}

func TestHashPublisherReplaceAll(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	hash := "test:hashreplaceall:" + time.Now().Format(time.RFC3339Nano)

	pub := client.NewHashPublisher(hash)

	// Set initial fields
	err = pub.SetMany(map[string]any{
		"old1": "value1",
		"old2": "value2",
	})
	if err != nil {
		t.Fatalf("SetMany() failed: %v", err)
	}

	// Replace all with new fields
	err = pub.ReplaceAll(map[string]any{
		"new1": "newvalue1",
		"new2": "newvalue2",
	})
	if err != nil {
		t.Fatalf("ReplaceAll() failed: %v", err)
	}

	// Verify old fields are gone and new ones exist
	all, err := pub.GetAll()
	if err != nil {
		t.Fatalf("GetAll() failed: %v", err)
	}
	if len(all) != 2 {
		t.Errorf("Expected 2 fields, got %d", len(all))
	}
	if _, ok := all["old1"]; ok {
		t.Error("old1 should be deleted")
	}
	if all["new1"] != "newvalue1" {
		t.Errorf("new1 = %q, want newvalue1", all["new1"])
	}
	if all["new2"] != "newvalue2" {
		t.Errorf("new2 = %q, want newvalue2", all["new2"])
	}

	// Test ReplaceAll with nil (should clear)
	err = pub.ReplaceAll(nil)
	if err != nil {
		t.Fatalf("ReplaceAll(nil) failed: %v", err)
	}

	all, err = pub.GetAll()
	if err != nil {
		t.Fatalf("GetAll() failed: %v", err)
	}
	if len(all) != 0 {
		t.Errorf("Hash should be empty after ReplaceAll(nil), got %d fields", len(all))
	}

	// Cleanup
	client.Del(hash)
}

func TestHashPublisherNoPublish(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	hash := "test:nopublish:" + time.Now().Format(time.RFC3339Nano)
	pub := client.NewHashPublisher(hash)

	// Subscribe to verify no publish happens
	pubsub := client.Raw().Subscribe(client.Context(), hash)
	defer pubsub.Close()

	ch := pubsub.Channel()
	time.Sleep(100 * time.Millisecond) // Wait for subscription

	// Set with NoPublish
	err = pub.Set("field1", "value1", NoPublish())
	if err != nil {
		t.Fatalf("Set() with NoPublish failed: %v", err)
	}

	// Verify value was set
	val, err := pub.Get("field1")
	if err != nil {
		t.Fatalf("Get() failed: %v", err)
	}
	if val != "value1" {
		t.Errorf("Get() = %q, want %q", val, "value1")
	}

	// Verify no message was published
	select {
	case msg := <-ch:
		t.Errorf("Unexpected message received: %v", msg.Payload)
	case <-time.After(200 * time.Millisecond):
		// Expected - no message
	}

	// Cleanup
	client.Del(hash)
}

func TestHashPublisherSetManyNoPublish(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	hash := "test:setmany-nopub:" + time.Now().Format(time.RFC3339Nano)
	pub := client.NewHashPublisher(hash)

	// Subscribe to verify no publish happens
	pubsub := client.Raw().Subscribe(client.Context(), hash)
	defer pubsub.Close()

	ch := pubsub.Channel()
	time.Sleep(100 * time.Millisecond)

	// SetMany with NoPublish
	err = pub.SetMany(map[string]any{
		"field1": "value1",
		"field2": "value2",
	}, NoPublish())
	if err != nil {
		t.Fatalf("SetMany() with NoPublish failed: %v", err)
	}

	// Verify values were set
	all, err := pub.GetAll()
	if err != nil {
		t.Fatalf("GetAll() failed: %v", err)
	}
	if all["field1"] != "value1" || all["field2"] != "value2" {
		t.Errorf("GetAll() = %v, want field1=value1, field2=value2", all)
	}

	// Verify no messages were published
	select {
	case msg := <-ch:
		t.Errorf("Unexpected message received: %v", msg.Payload)
	case <-time.After(200 * time.Millisecond):
		// Expected - no message
	}

	// Cleanup
	client.Del(hash)
}

func TestHashPublisherSetManyPublishOne(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	hash := "test:setmany-pubone:" + time.Now().Format(time.RFC3339Nano)
	pub := client.NewHashPublisher(hash)

	// Subscribe
	pubsub := client.Raw().Subscribe(client.Context(), hash)
	defer pubsub.Close()

	ch := pubsub.Channel()
	time.Sleep(100 * time.Millisecond)

	// SetManyPublishOne
	err = pub.SetManyPublishOne(map[string]any{
		"field1": "value1",
		"field2": "value2",
		"field3": "value3",
	}, "batch-update")
	if err != nil {
		t.Fatalf("SetManyPublishOne() failed: %v", err)
	}

	// Verify values were set
	all, err := pub.GetAll()
	if err != nil {
		t.Fatalf("GetAll() failed: %v", err)
	}
	if len(all) != 3 {
		t.Errorf("Expected 3 fields, got %d", len(all))
	}

	// Verify only one message was published
	var messages []string
	timeout := time.After(500 * time.Millisecond)
	for {
		select {
		case msg := <-ch:
			messages = append(messages, msg.Payload)
		case <-timeout:
			goto done
		}
	}
done:

	if len(messages) != 1 {
		t.Errorf("Expected 1 message, got %d: %v", len(messages), messages)
	}
	if len(messages) > 0 && messages[0] != "batch-update" {
		t.Errorf("Message = %q, want %q", messages[0], "batch-update")
	}

	// Cleanup
	client.Del(hash)
}

func TestHashPublisherGetAll(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	hash := "test:getall:" + time.Now().Format(time.RFC3339Nano)
	pub := client.NewHashPublisher(hash)

	// Set multiple fields
	err = pub.SetMany(map[string]any{
		"field1": "value1",
		"field2": "value2",
		"field3": "value3",
	})
	if err != nil {
		t.Fatalf("SetMany() failed: %v", err)
	}

	// Get individual fields
	val, err := pub.Get("field1")
	if err != nil {
		t.Fatalf("Get(field1) failed: %v", err)
	}
	if val != "value1" {
		t.Errorf("Get(field1) = %q, want value1", val)
	}

	val, err = pub.Get("field2")
	if err != nil {
		t.Fatalf("Get(field2) failed: %v", err)
	}
	if val != "value2" {
		t.Errorf("Get(field2) = %q, want value2", val)
	}

	// Get nonexistent field
	val, err = pub.Get("nonexistent")
	if err == nil {
		t.Error("Get(nonexistent) should return error")
	}

	// GetAll
	all, err := pub.GetAll()
	if err != nil {
		t.Fatalf("GetAll() failed: %v", err)
	}
	if len(all) != 3 {
		t.Errorf("GetAll() returned %d fields, want 3", len(all))
	}
	if all["field1"] != "value1" || all["field2"] != "value2" || all["field3"] != "value3" {
		t.Errorf("GetAll() = %v, unexpected values", all)
	}

	// Cleanup
	client.Del(hash)
}

func TestHashWatcherFetch(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	hash := "test:watcherfetch:" + time.Now().Format(time.RFC3339Nano)

	// Populate hash
	pub := client.NewHashPublisher(hash)
	err = pub.SetMany(map[string]any{
		"state":  "ready",
		"charge": "75",
	})
	if err != nil {
		t.Fatalf("SetMany() failed: %v", err)
	}

	watcher := client.NewHashWatcher(hash)

	// Fetch single field
	val, err := watcher.Fetch("state")
	if err != nil {
		t.Fatalf("Fetch(state) failed: %v", err)
	}
	if val != "ready" {
		t.Errorf("Fetch(state) = %q, want ready", val)
	}

	// Fetch nonexistent field
	_, err = watcher.Fetch("nonexistent")
	if err == nil {
		t.Error("Fetch(nonexistent) should return error")
	}

	// FetchAll
	all, err := watcher.FetchAll()
	if err != nil {
		t.Fatalf("FetchAll() failed: %v", err)
	}
	if len(all) != 2 {
		t.Errorf("FetchAll() returned %d fields, want 2", len(all))
	}
	if all["state"] != "ready" || all["charge"] != "75" {
		t.Errorf("FetchAll() = %v, unexpected values", all)
	}

	// Cleanup
	client.Del(hash)
}

func TestOnFieldTyped(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	hash := "test:onfieldtyped:" + time.Now().Format(time.RFC3339Nano)

	type Config struct {
		MaxSpeed int  `json:"max_speed"`
		Enabled  bool `json:"enabled"`
	}

	received := make(chan Config, 1)

	watcher := client.NewHashWatcher(hash)
	OnFieldTyped(watcher, "config", func(cfg Config) error {
		received <- cfg
		return nil
	})

	err = watcher.Start()
	if err != nil {
		t.Fatalf("Start() failed: %v", err)
	}
	defer watcher.Stop()

	time.Sleep(100 * time.Millisecond)

	// Publish JSON config
	pub := client.NewHashPublisher(hash)
	err = pub.Set("config", `{"max_speed":25,"enabled":true}`)
	if err != nil {
		t.Fatalf("Set() failed: %v", err)
	}

	select {
	case cfg := <-received:
		if cfg.MaxSpeed != 25 {
			t.Errorf("MaxSpeed = %d, want 25", cfg.MaxSpeed)
		}
		if !cfg.Enabled {
			t.Error("Enabled = false, want true")
		}
	case <-time.After(2 * time.Second):
		t.Error("Timeout waiting for typed config")
	}

	// Cleanup
	client.Del(hash)
}
