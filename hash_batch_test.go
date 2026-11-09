package redis_ipc

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"
)

func hsetCalls(t *testing.T, c *Client) int {
	t.Helper()
	info, err := c.redis.Info(context.Background(), "commandstats").Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(info, "\n") {
		if strings.HasPrefix(line, "cmdstat_hset:") {
			n, _ := strconv.Atoi(strings.TrimPrefix(strings.SplitN(strings.SplitN(line, ",", 2)[0], "=", 2)[1], ""))
			return n
		}
	}
	return 0
}

// A batch of fields reaches Redis as one HSET, not one per field.
func TestSetManyIsOneHSET(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()
	hash := "test:batch:" + time.Now().Format(time.RFC3339Nano)
	defer client.Del(hash)
	pub := client.NewHashPublisher(hash)
	fields := map[string]any{"a": 1, "b": "two", "c": 3.5, "d": true, "e": "x"}

	for name, call := range map[string]func() error{
		"SetMany":           func() error { return pub.SetMany(fields, Sync()) },
		"SetManyPublishOne": func() error { return pub.SetManyPublishOne(fields, "n", Sync()) },
		"ReplaceAll":        func() error { return pub.ReplaceAll(fields, Sync()) },
		"SetWithTimestamp":  func() error { return pub.SetWithTimestamp("a", 1, Sync()) },
	} {
		before := hsetCalls(t, client)
		if err := call(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got := hsetCalls(t, client) - before; got != 1 {
			t.Errorf("%s issued %d HSET commands, want 1", name, got)
		}
	}
	got, err := client.redis.HGetAll(context.Background(), hash).Result()
	if err != nil {
		t.Fatal(err)
	}
	if got["a"] != "1" || got["b"] != "two" || got["c"] != "3.5" || got["d"] != "true" || got["e"] != "x" || got["a:timestamp"] == "" {
		t.Errorf("unexpected hash contents: %v", got)
	}
}
