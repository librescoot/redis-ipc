package redis_ipc

import (
	"fmt"
	"net"
	"os/exec"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func reconnectRedis(t *testing.T) int {
	t.Helper()
	if _, err := exec.LookPath("redis-server"); err != nil {
		t.Skip("redis-server not installed")
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	cmd := exec.Command("redis-server", "--bind", "127.0.0.1", "--port", strconv.Itoa(port), "--save", "", "--appendonly", "no", "--dir", t.TempDir())
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	if !waitFor(t, 5*time.Second, func() bool {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 100*time.Millisecond)
		if err != nil {
			return false
		}
		_ = conn.Close()
		return true
	}) {
		t.Fatal("redis-server did not start")
	}
	return port
}

func TestHashWatchersResyncAfterPubsubReconnect(t *testing.T) {
	port := reconnectRedis(t)
	client, err := New(WithAddress("127.0.0.1"), WithPort(port))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.HSet("reconnect:test", "state", "old"); err != nil {
		t.Fatal(err)
	}
	if err := client.HSet("reconnect:test", "deleted", "present"); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	got := map[string]string{}
	var calls int
	w := client.NewHashWatcher("reconnect:test")
	w.OnAny(func(field, value string) error {
		mu.Lock()
		defer mu.Unlock()
		got[field] = value
		calls++
		return nil
	})
	if err := w.StartWithSync(); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()
	var second atomic.Int32
	w2 := client.NewHashWatcher("reconnect:test")
	w2.OnField("state", func(value string) error {
		if value == "new" {
			second.Add(1)
		}
		return nil
	})
	if err := w2.StartWithSync(); err != nil {
		t.Fatal(err)
	}
	defer w2.Stop()
	var eventsOnly atomic.Int32
	w3 := client.NewHashWatcher("reconnect:test")
	w3.OnField("state", func(string) error { eventsOnly.Add(1); return nil })
	if err := w3.Start(); err != nil {
		t.Fatal(err)
	}
	defer w3.Stop()
	mu.Lock()
	initial := calls
	mu.Unlock()
	if initial != 2 {
		t.Fatalf("initial callbacks = %d, want 2", initial)
	}

	// Update the hash without publishing, then disconnect the subscription.
	// Only reconnect reconciliation can deliver these values.
	if err := client.HSet("reconnect:test", "state", "new"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Do("HDEL", "reconnect:test", "deleted"); err != nil {
		t.Fatal(err)
	}
	if err := client.HSet("reconnect:test", "empty", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Do("CLIENT", "KILL", "TYPE", "pubsub"); err != nil {
		t.Fatal(err)
	}
	if !waitFor(t, 8*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return got["state"] == "new" && got["deleted"] == "" && got["empty"] == "" && calls >= initial+3 && second.Load() == 1
	}) {
		mu.Lock()
		defer mu.Unlock()
		t.Fatalf("reconnect snapshot missing: values=%v calls=%d", got, calls)
	}
	if got := eventsOnly.Load(); got != 0 {
		t.Fatalf("Start watcher received %d snapshot callbacks", got)
	}
	if err := client.NewHashPublisher("reconnect:test").Set("state", "live", Sync()); err != nil {
		t.Fatal(err)
	}
	if !waitFor(t, 3*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return got["state"] == "live" }) {
		t.Fatal("live notification missing after reconnect")
	}
}
