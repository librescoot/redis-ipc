package redis_ipc

import (
	"context"
	"testing"
	"time"
)

func TestTxHashOnly(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	hash := "test:tx:hash:" + time.Now().Format(time.RFC3339Nano)
	defer client.Del(hash)

	hp := client.NewHashPublisher(hash)

	tx := client.NewTx()
	changed := tx.HashSetManyIfChanged(hp, map[string]any{
		"state":   "active",
		"voltage": "52000",
	})
	if len(changed) != 2 {
		t.Errorf("expected 2 changed fields, got %d", len(changed))
	}

	if err := tx.Exec(); err != nil {
		t.Fatalf("Exec() failed: %v", err)
	}

	all, err := hp.GetAll()
	if err != nil {
		t.Fatalf("GetAll() failed: %v", err)
	}
	if all["state"] != "active" {
		t.Errorf("state = %q, want active", all["state"])
	}
	if all["voltage"] != "52000" {
		t.Errorf("voltage = %q, want 52000", all["voltage"])
	}

	// Second Tx with one unchanged field
	tx2 := client.NewTx()
	changed2 := tx2.HashSetManyIfChanged(hp, map[string]any{
		"state":   "active", // unchanged
		"voltage": "48000",  // changed
	})
	if err := tx2.Exec(); err != nil {
		t.Fatalf("Exec() failed: %v", err)
	}
	if len(changed2) != 1 || changed2[0] != "voltage" {
		t.Errorf("expected [voltage] changed, got %v", changed2)
	}
}

func TestTxHashAndFault(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	hash := "test:tx:hashfault:" + time.Now().Format(time.RFC3339Nano)
	faultKey := hash + ":fault"
	defer client.Del(hash, faultKey)

	hp := client.NewHashPublisher(hash)
	fs := client.NewFaultSet(faultKey, hash, "fault")

	tx := client.NewTx()
	tx.HashSetManyIfChanged(hp, map[string]any{"state": "fault"})
	tx.FaultAdd(fs, 42)

	if err := tx.Exec(); err != nil {
		t.Fatalf("Exec() failed: %v", err)
	}

	val, err := hp.Get("state")
	if err != nil {
		t.Fatalf("Get() failed: %v", err)
	}
	if val != "fault" {
		t.Errorf("state = %q, want fault", val)
	}

	has, err := fs.Has(42)
	if err != nil {
		t.Fatalf("Has() failed: %v", err)
	}
	if !has {
		t.Error("fault code 42 should be present")
	}

	// Remove fault in next Tx
	tx2 := client.NewTx()
	tx2.HashSetManyIfChanged(hp, map[string]any{"state": "ready"})
	tx2.FaultRemove(fs, 42)

	if err := tx2.Exec(); err != nil {
		t.Fatalf("Exec() failed: %v", err)
	}

	has, err = fs.Has(42)
	if err != nil {
		t.Fatalf("Has() failed: %v", err)
	}
	if has {
		t.Error("fault code 42 should have been removed")
	}
}

func TestTxFaultUpdate(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	key := "test:tx:faultupdate:" + time.Now().Format(time.RFC3339Nano)
	defer client.Del(key)

	fs := client.NewFaultSet(key, "chan:"+key, "fault")

	// Seed some faults
	if err := fs.Add(1); err != nil {
		t.Fatalf("Add() failed: %v", err)
	}
	if err := fs.Add(2); err != nil {
		t.Fatalf("Add() failed: %v", err)
	}

	// Atomically add 3, remove 1
	tx := client.NewTx()
	tx.FaultUpdate(fs, []int{3}, []int{1})
	if err := tx.Exec(); err != nil {
		t.Fatalf("Exec() failed: %v", err)
	}

	has1, _ := fs.Has(1)
	has2, _ := fs.Has(2)
	has3, _ := fs.Has(3)

	if has1 {
		t.Error("fault 1 should have been removed")
	}
	if !has2 {
		t.Error("fault 2 should still be present")
	}
	if !has3 {
		t.Error("fault 3 should have been added")
	}
}

func TestTxHashFaultAndStream(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	hash := "test:tx:full:" + time.Now().Format(time.RFC3339Nano)
	faultKey := hash + ":fault"
	streamKey := hash + ":events"
	defer client.Del(hash, faultKey, streamKey)

	hp := client.NewHashPublisher(hash)
	fs := client.NewFaultSet(faultKey, hash, "fault")
	sp := client.NewStreamPublisher(streamKey)

	tx := client.NewTx()
	tx.HashSetManyIfChanged(hp, map[string]any{"state": "fault", "code": "7"})
	tx.FaultAdd(fs, 7)
	tx.StreamAdd(sp, map[string]any{"event": "fault_detected", "code": "7"})

	if err := tx.Exec(); err != nil {
		t.Fatalf("Exec() failed: %v", err)
	}

	val, err := hp.Get("state")
	if err != nil || val != "fault" {
		t.Errorf("state = %q (err=%v), want fault", val, err)
	}

	has, err := fs.Has(7)
	if err != nil || !has {
		t.Errorf("fault 7 should be present (err=%v)", err)
	}

	msgs, err := client.Raw().XRange(client.Context(), streamKey, "-", "+").Result()
	if err != nil || len(msgs) == 0 {
		t.Errorf("expected stream entry (err=%v, len=%d)", err, len(msgs))
	}
}

func TestTxCacheNotUpdatedOnFailure(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	hash := "test:tx:failure:" + time.Now().Format(time.RFC3339Nano)
	defer client.Del(hash)

	hp := client.NewHashPublisher(hash)

	// Pre-warm cache: set "state" = "idle"
	if _, err := hp.SetManyIfChanged(map[string]any{"state": "idle"}); err != nil {
		t.Fatalf("SetManyIfChanged() failed: %v", err)
	}

	// Create Tx with a cancelled context so Exec will fail
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	tx := client.NewTxWithContext(ctx)
	changed := tx.HashSetManyIfChanged(hp, map[string]any{"state": "active"})
	if len(changed) != 1 {
		t.Errorf("expected 1 changed field pre-exec, got %d", len(changed))
	}

	err = tx.Exec()
	if err == nil {
		t.Fatal("expected Exec to fail with cancelled context")
	}

	// Cache should still reflect "idle" — verify by checking a new Tx sees the field as changed
	tx2 := client.NewTx()
	changed2 := tx2.HashSetManyIfChanged(hp, map[string]any{"state": "active"})
	if len(changed2) != 1 {
		t.Errorf("cache should not have been updated after failed Exec; expected 1 changed, got %d", len(changed2))
	}
}

func TestTxEmpty(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	tx := client.NewTx()
	if err := tx.Exec(); err != nil {
		t.Errorf("empty Tx.Exec() should not fail, got: %v", err)
	}
}

func TestTxMultipleHashPublishers(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	ts := time.Now().Format(time.RFC3339Nano)
	hash1 := "test:tx:multi1:" + ts
	hash2 := "test:tx:multi2:" + ts
	defer client.Del(hash1, hash2)

	hp1 := client.NewHashPublisher(hash1)
	hp2 := client.NewHashPublisher(hash2)

	tx := client.NewTx()
	tx.HashSetManyIfChanged(hp1, map[string]any{"a": "1"})
	tx.HashSetManyIfChanged(hp2, map[string]any{"b": "2"})

	if err := tx.Exec(); err != nil {
		t.Fatalf("Exec() failed: %v", err)
	}

	v1, err := hp1.Get("a")
	if err != nil || v1 != "1" {
		t.Errorf("hp1.a = %q (err=%v), want 1", v1, err)
	}

	v2, err := hp2.Get("b")
	if err != nil || v2 != "2" {
		t.Errorf("hp2.b = %q (err=%v), want 2", v2, err)
	}
}
