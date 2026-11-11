package redis_ipc

import (
	"fmt"
	"regexp"
	"strconv"
	"testing"
	"time"
)

func TestLocalIdleClientDoesNotPingEveryFewSeconds(t *testing.T) {
	port := reconnectRedis(t)
	client, err := New(WithAddress("127.0.0.1"), WithPort(port))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	watcher := client.NewHashWatcher("idle:test")
	if err := watcher.StartWithSync(); err != nil {
		t.Fatal(err)
	}
	defer watcher.Stop()
	count := func() int {
		result, err := client.Do("INFO", "commandstats")
		if err != nil {
			t.Fatal(err)
		}
		matches := regexp.MustCompile(`cmdstat_ping:calls=([0-9]+)`).FindStringSubmatch(fmt.Sprint(result))
		if len(matches) != 2 {
			t.Fatalf("no PING stats: %s", result)
		}
		n, err := strconv.Atoi(matches[1])
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	before := count()
	time.Sleep(7 * time.Second)
	if after := count(); after != before {
		t.Fatalf("idle local PING count increased from %d to %d", before, after)
	}
}

func TestHealthCheckPolicy(t *testing.T) {
	for _, tc := range []struct {
		name    string
		opts    []Option
		monitor time.Duration
		pubsub  time.Duration
	}{
		{"default localhost", nil, 30 * time.Second, 30 * time.Second},
		{"IPv4 loopback", []Option{WithAddress("127.0.0.1")}, 30 * time.Second, 30 * time.Second},
		{"IPv6 loopback", []Option{WithAddress("::1")}, 30 * time.Second, 30 * time.Second},
		{"dashboard link", []Option{WithAddress("192.168.7.1")}, 5 * time.Second, 3 * time.Second},
		{"unknown hostname", []Option{WithAddress("redis.example")}, 5 * time.Second, 3 * time.Second},
		{"explicit interval", []Option{WithRetryInterval(time.Second)}, time.Second, 30 * time.Second},
		{"disconnect callback", []Option{WithOnDisconnect(func(error) {})}, 5 * time.Second, 30 * time.Second},
		{"connect callback", []Option{WithOnConnect(func() {})}, 5 * time.Second, 30 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := defaultOptions()
			for _, opt := range tc.opts {
				opt(o)
			}
			o.configureHealthChecks()
			if o.retryInterval != tc.monitor || o.pubsubCheckInterval != tc.pubsub {
				t.Fatalf("monitor=%s pubsub=%s, want %s and %s", o.retryInterval, o.pubsubCheckInterval, tc.monitor, tc.pubsub)
			}
		})
	}
}
