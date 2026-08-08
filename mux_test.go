package redis_ipc

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// pubsubConnCount returns how many client connections the server
// currently has in subscriber mode (sub= greater than zero).
func pubsubConnCount(t *testing.T, c *Client) int {
	t.Helper()
	raw, err := c.Do("CLIENT", "LIST")
	if err != nil {
		t.Fatalf("CLIENT LIST failed: %v", err)
	}
	n := 0
	for _, line := range strings.Split(fmt.Sprint(raw), "\n") {
		for _, field := range strings.Fields(line) {
			if strings.HasPrefix(field, "sub=") && field != "sub=0" {
				n++
			}
		}
	}
	return n
}

// blockedClients returns the server's count of clients parked in a
// blocking command (BRPOP and friends).
func blockedClients(t *testing.T, c *Client) int {
	t.Helper()
	raw, err := c.Do("INFO", "clients")
	if err != nil {
		t.Fatalf("INFO clients failed: %v", err)
	}
	for _, line := range strings.Split(fmt.Sprint(raw), "\n") {
		line = strings.TrimSpace(line)
		if v, ok := strings.CutPrefix(line, "blocked_clients:"); ok {
			var n int
			if _, err := fmt.Sscanf(v, "%d", &n); err != nil {
				t.Fatalf("parse blocked_clients %q: %v", v, err)
			}
			return n
		}
	}
	t.Fatal("blocked_clients not found in INFO clients")
	return 0
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cond()
}

// TestPubsubMuxSharesOneConnection is the point of the whole exercise:
// many watchers, one pub/sub connection.
func TestPubsubMuxSharesOneConnection(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	before := pubsubConnCount(t, client)

	suffix := time.Now().Format(time.RFC3339Nano)
	watchers := make([]*HashWatcher, 0, 8)
	for i := 0; i < 8; i++ {
		hash := fmt.Sprintf("test:mux:%s:%d", suffix, i)
		hw := client.NewHashWatcher(hash)
		hw.OnAny(func(field, value string) error { return nil })
		if err := hw.Start(); err != nil {
			t.Fatalf("watcher %d Start() failed: %v", i, err)
		}
		watchers = append(watchers, hw)
	}

	after := pubsubConnCount(t, client)
	if delta := after - before; delta != 1 {
		t.Errorf("8 watchers added %d pub/sub connections, want 1", delta)
	}

	for _, hw := range watchers {
		if err := hw.Stop(); err != nil {
			t.Errorf("Stop() failed: %v", err)
		}
	}
}

// TestPubsubMuxSharedChannel checks the refcounting: two watchers on the
// same channel both get messages, and stopping one leaves the other
// subscribed.
func TestPubsubMuxSharedChannel(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	hash := "test:muxshared:" + time.Now().Format(time.RFC3339Nano)
	defer client.Del(hash)

	var firstHits, secondHits atomic.Int32

	first := client.NewHashWatcher(hash)
	first.OnField("state", func(value string) error {
		firstHits.Add(1)
		return nil
	})
	if err := first.Start(); err != nil {
		t.Fatalf("first Start() failed: %v", err)
	}

	second := client.NewHashWatcher(hash)
	second.OnField("state", func(value string) error {
		secondHits.Add(1)
		return nil
	})
	if err := second.Start(); err != nil {
		t.Fatalf("second Start() failed: %v", err)
	}
	defer second.Stop()

	pub := client.NewHashPublisher(hash)
	if err := pub.Set("state", "one", Sync()); err != nil {
		t.Fatalf("Set() failed: %v", err)
	}

	if !waitFor(t, 2*time.Second, func() bool {
		return firstHits.Load() == 1 && secondHits.Load() == 1
	}) {
		t.Fatalf("both watchers should have fired once, got first=%d second=%d",
			firstHits.Load(), secondHits.Load())
	}

	// Dropping one subscriber must not unsubscribe the channel.
	if err := first.Stop(); err != nil {
		t.Fatalf("first Stop() failed: %v", err)
	}

	if err := pub.Set("state", "two", Sync()); err != nil {
		t.Fatalf("Set() failed: %v", err)
	}

	if !waitFor(t, 2*time.Second, func() bool { return secondHits.Load() == 2 }) {
		t.Errorf("surviving watcher stopped receiving: second=%d", secondHits.Load())
	}
	if got := firstHits.Load(); got != 1 {
		t.Errorf("stopped watcher still firing: first=%d, want 1", got)
	}
}

// TestPubsubMuxDoubleStop checks that a stale subscription handle is
// harmless: unsubscribing it twice must not disturb a live subscriber on
// the same channel.
func TestPubsubMuxDoubleStop(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	channel := "test:muxdouble:" + time.Now().Format(time.RFC3339Nano)

	first, err := Subscribe(client, channel, func(msg string) error { return nil })
	if err != nil {
		t.Fatalf("Subscribe() failed: %v", err)
	}
	if err := first.Unsubscribe(); err != nil {
		t.Fatalf("Unsubscribe() failed: %v", err)
	}

	received := make(chan string, 1)
	second, err := Subscribe(client, channel, func(msg string) error {
		received <- msg
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe() failed: %v", err)
	}
	defer second.Unsubscribe()

	// The stale handle must not disturb the live subscription.
	if err := first.Unsubscribe(); err != nil {
		t.Fatalf("repeated Unsubscribe() failed: %v", err)
	}

	if err := PublishTyped(client, channel, "hello", Sync()); err != nil {
		t.Fatalf("PublishTyped() failed: %v", err)
	}
	select {
	case msg := <-received:
		if msg != "hello" {
			t.Errorf("got %q, want %q", msg, "hello")
		}
	case <-time.After(2 * time.Second):
		t.Error("repeated Unsubscribe dropped the live subscription")
	}
}

// TestPubsubMuxUnsubscribesLastWatcher verifies the channel is actually
// dropped from the shared connection once nobody wants it.
func TestPubsubMuxUnsubscribesLastWatcher(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	hash := "test:muxlast:" + time.Now().Format(time.RFC3339Nano)

	hw := client.NewHashWatcher(hash)
	hw.OnAny(func(field, value string) error { return nil })
	if err := hw.Start(); err != nil {
		t.Fatalf("Start() failed: %v", err)
	}
	if err := hw.Stop(); err != nil {
		t.Fatalf("Stop() failed: %v", err)
	}

	if !waitFor(t, 2*time.Second, func() bool {
		raw, err := client.Do("PUBSUB", "CHANNELS", hash)
		if err != nil {
			return false
		}
		list, ok := raw.([]any)
		return ok && len(list) == 0
	}) {
		t.Errorf("channel %s still subscribed after the last watcher stopped", hash)
	}
}

// TestPubsubMuxStopStartRace races the last watcher's Stop against a new
// watcher's Start on the same channel. Either order is fine, but the
// UNSUBSCRIBE must never land after the new SUBSCRIBE and leave the new
// watcher silently deaf.
func TestPubsubMuxStopStartRace(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	hash := "test:muxrace:" + time.Now().Format(time.RFC3339Nano)
	defer client.Del(hash)
	pub := client.NewHashPublisher(hash)

	for i := 0; i < 25; i++ {
		outgoing := client.NewHashWatcher(hash)
		outgoing.OnAny(func(field, value string) error { return nil })
		if err := outgoing.Start(); err != nil {
			t.Fatalf("round %d: Start() failed: %v", i, err)
		}

		hits := make(chan string, 4)
		incoming := client.NewHashWatcher(hash)
		incoming.OnField("state", func(value string) error {
			select {
			case hits <- value:
			default:
			}
			return nil
		})

		var wg sync.WaitGroup
		wg.Add(2)
		start := make(chan struct{})
		var startErr error
		go func() {
			defer wg.Done()
			<-start
			_ = outgoing.Stop()
		}()
		go func() {
			defer wg.Done()
			<-start
			startErr = incoming.Start()
		}()
		close(start)
		wg.Wait()

		if startErr != nil {
			t.Fatalf("round %d: Start() failed: %v", i, startErr)
		}

		want := fmt.Sprintf("v%d", i)
		if err := pub.Set("state", want, Sync()); err != nil {
			t.Fatalf("round %d: Set() failed: %v", i, err)
		}

		select {
		case got := <-hits:
			if got != want {
				t.Fatalf("round %d: got %q, want %q", i, got, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("round %d: new watcher never received; a concurrent Stop dropped its subscription", i)
		}
		_ = incoming.Stop()
	}
}

// TestStartWithSyncOrdering covers the subscribe-then-fetch guarantee:
// nothing published after the subscription may be lost while the initial
// HGETALL runs.
func TestStartWithSyncOrdering(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	hash := "test:muxsync:" + time.Now().Format(time.RFC3339Nano)
	defer client.Del(hash)

	pub := client.NewHashPublisher(hash)
	if err := pub.Set("state", "initial", Sync()); err != nil {
		t.Fatalf("Set() failed: %v", err)
	}

	var mu sync.Mutex
	var seen []string

	hw := client.NewHashWatcher(hash)
	hw.OnField("state", func(value string) error {
		mu.Lock()
		seen = append(seen, value)
		mu.Unlock()
		return nil
	})
	if err := hw.StartWithSync(); err != nil {
		t.Fatalf("StartWithSync() failed: %v", err)
	}
	defer hw.Stop()

	if err := pub.Set("state", "live", Sync()); err != nil {
		t.Fatalf("Set() failed: %v", err)
	}

	if !waitFor(t, 2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) >= 2
	}) {
		mu.Lock()
		t.Fatalf("expected initial sync plus live update, got %v", seen)
	}

	mu.Lock()
	defer mu.Unlock()
	if seen[0] != "initial" {
		t.Errorf("first handler call = %q, want the synced value %q", seen[0], "initial")
	}
	if seen[len(seen)-1] != "live" {
		t.Errorf("last handler call = %q, want %q", seen[len(seen)-1], "live")
	}
}

// TestPubsubMuxSlowSubscriberIsolated makes sure one wedged handler does
// not stall the other channels sharing the connection.
func TestPubsubMuxSlowSubscriberIsolated(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	suffix := time.Now().Format(time.RFC3339Nano)
	slowHash := "test:muxslow:" + suffix
	fastHash := "test:muxfast:" + suffix
	defer client.Del(slowHash)
	defer client.Del(fastHash)

	blocked := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once

	slow := client.NewHashWatcher(slowHash)
	slow.OnField("state", func(value string) error {
		once.Do(func() { close(blocked) })
		<-release
		return nil
	})
	if err := slow.Start(); err != nil {
		t.Fatalf("slow Start() failed: %v", err)
	}
	defer slow.Stop()

	var fastHits atomic.Int32
	fast := client.NewHashWatcher(fastHash)
	fast.OnField("state", func(value string) error {
		fastHits.Add(1)
		return nil
	})
	if err := fast.Start(); err != nil {
		t.Fatalf("fast Start() failed: %v", err)
	}
	defer fast.Stop()

	if err := client.NewHashPublisher(slowHash).Set("state", "wedged", Sync()); err != nil {
		t.Fatalf("Set() failed: %v", err)
	}
	select {
	case <-blocked:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("slow handler never ran")
	}

	if err := client.NewHashPublisher(fastHash).Set("state", "ok", Sync()); err != nil {
		close(release)
		t.Fatalf("Set() failed: %v", err)
	}

	got := waitFor(t, 2*time.Second, func() bool { return fastHits.Load() == 1 })
	close(release)
	if !got {
		t.Error("fast watcher was blocked by the slow one on the shared connection")
	}
}

// TestQueueMuxSharesOneConnection: many queues, one blocking connection.
func TestQueueMuxSharesOneConnection(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	before := blockedClients(t, client)

	suffix := time.Now().Format(time.RFC3339Nano)
	handlers := make([]*QueueHandler[string], 0, 6)
	for i := 0; i < 6; i++ {
		queue := fmt.Sprintf("test:muxq:%s:%d", suffix, i)
		handlers = append(handlers, HandleRequests(client, queue, func(string) error { return nil }))
	}

	if !waitFor(t, 3*time.Second, func() bool { return blockedClients(t, client) == before+1 }) {
		t.Errorf("6 queue handlers left %d blocked clients, want %d",
			blockedClients(t, client), before+1)
	}

	for _, qh := range handlers {
		qh.Stop()
	}
}

// TestQueueMuxDeliversPerQueue checks that the shared BRPOP loop routes
// each message to the handler that registered its key.
func TestQueueMuxDeliversPerQueue(t *testing.T) {
	client, err := New(WithAddress("localhost"), WithCodec(StringCodec{}))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	suffix := time.Now().Format(time.RFC3339Nano)
	queues := []string{
		"test:muxroute:" + suffix + ":a",
		"test:muxroute:" + suffix + ":b",
		"test:muxroute:" + suffix + ":c",
	}

	var mu sync.Mutex
	got := map[string][]string{}

	for _, q := range queues {
		q := q
		defer client.Del(q)
		qh := HandleRequests(client, q, func(msg string) error {
			mu.Lock()
			got[q] = append(got[q], msg)
			mu.Unlock()
			return nil
		})
		defer qh.Stop()
	}

	for i, q := range queues {
		if err := SendRequest(client, q, fmt.Sprintf("msg-%d", i)); err != nil {
			t.Fatalf("SendRequest(%s) failed: %v", q, err)
		}
	}

	if !waitFor(t, 5*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) == len(queues)
	}) {
		mu.Lock()
		t.Fatalf("not every queue was served: %v", got)
	}

	mu.Lock()
	defer mu.Unlock()
	for i, q := range queues {
		want := fmt.Sprintf("msg-%d", i)
		if len(got[q]) != 1 || got[q][0] != want {
			t.Errorf("queue %s got %v, want [%s]", q, got[q], want)
		}
	}
}

// TestQueueMuxLateRegistration exercises the wake path: a queue
// registered while the shared BRPOP is already blocked must start
// receiving without waiting out the blocking timeout.
func TestQueueMuxLateRegistration(t *testing.T) {
	client, err := New(WithAddress("localhost"), WithCodec(StringCodec{}))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	suffix := time.Now().Format(time.RFC3339Nano)
	early := "test:muxlate:" + suffix + ":early"
	late := "test:muxlate:" + suffix + ":late"
	defer client.Del(early)
	defer client.Del(late)

	first := HandleRequests(client, early, func(string) error { return nil })
	defer first.Stop()

	// Let the loop settle into its blocking BRPOP.
	time.Sleep(500 * time.Millisecond)

	received := make(chan string, 1)
	second := HandleRequests(client, late, func(msg string) error {
		received <- msg
		return nil
	})
	defer second.Stop()

	if err := SendRequest(client, late, "hello"); err != nil {
		t.Fatalf("SendRequest() failed: %v", err)
	}

	select {
	case msg := <-received:
		if msg != "hello" {
			t.Errorf("got %q, want %q", msg, "hello")
		}
	case <-time.After(5 * time.Second):
		t.Error("late-registered queue was not picked up by the shared BRPOP loop")
	}
}

// TestQueueMuxStopLeavesOthersRunning verifies unregistering one queue
// does not disturb the rest of the key set.
func TestQueueMuxStopLeavesOthersRunning(t *testing.T) {
	client, err := New(WithAddress("localhost"), WithCodec(StringCodec{}))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	suffix := time.Now().Format(time.RFC3339Nano)
	stopped := "test:muxstop:" + suffix + ":stopped"
	alive := "test:muxstop:" + suffix + ":alive"
	defer client.Del(stopped)
	defer client.Del(alive)

	doomed := HandleRequests(client, stopped, func(string) error { return nil })

	received := make(chan string, 1)
	survivor := HandleRequests(client, alive, func(msg string) error {
		received <- msg
		return nil
	})
	defer survivor.Stop()

	doomed.Stop()

	if err := SendRequest(client, alive, "still here"); err != nil {
		t.Fatalf("SendRequest() failed: %v", err)
	}
	select {
	case msg := <-received:
		if msg != "still here" {
			t.Errorf("got %q, want %q", msg, "still here")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("surviving queue stopped receiving after a sibling was stopped")
	}

	// A stopped consumer must leave its messages in Redis.
	if err := SendRequest(client, stopped, "orphan"); err != nil {
		t.Fatalf("SendRequest() failed: %v", err)
	}
	time.Sleep(500 * time.Millisecond)
	n, err := client.Exists(stopped)
	if err != nil {
		t.Fatalf("Exists() failed: %v", err)
	}
	if n != 1 {
		t.Error("message pushed to a stopped queue was consumed anyway")
	}
}

// TestQueueMuxSlowHandlerIsolated: a wedged handler must not starve the
// other queues on the shared loop.
func TestQueueMuxSlowHandlerIsolated(t *testing.T) {
	client, err := New(WithAddress("localhost"), WithCodec(StringCodec{}))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	suffix := time.Now().Format(time.RFC3339Nano)
	slowQ := "test:muxqslow:" + suffix
	fastQ := "test:muxqfast:" + suffix
	defer client.Del(slowQ)
	defer client.Del(fastQ)

	blocked := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once

	slow := HandleRequests(client, slowQ, func(string) error {
		once.Do(func() { close(blocked) })
		<-release
		return nil
	})
	defer slow.Stop()

	received := make(chan string, 1)
	fast := HandleRequests(client, fastQ, func(msg string) error {
		received <- msg
		return nil
	})
	defer fast.Stop()

	if err := SendRequest(client, slowQ, "wedge"); err != nil {
		t.Fatalf("SendRequest() failed: %v", err)
	}
	select {
	case <-blocked:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("slow handler never ran")
	}

	if err := SendRequest(client, fastQ, "ok"); err != nil {
		close(release)
		t.Fatalf("SendRequest() failed: %v", err)
	}
	select {
	case <-received:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("fast queue was blocked by the slow one on the shared loop")
	}
	close(release)
}

// TestQueueMuxFairness pushes a burst onto one queue and checks another
// queue still gets served: BRPOP always takes the earliest non-empty
// key, so the mux rotates the key order.
func TestQueueMuxFairness(t *testing.T) {
	client, err := New(WithAddress("localhost"), WithCodec(StringCodec{}))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer client.Close()

	suffix := time.Now().Format(time.RFC3339Nano)
	// "a" sorts before "z", so without rotation the busy queue would
	// always win the BRPOP race.
	busy := "test:muxfair:" + suffix + ":a"
	quiet := "test:muxfair:" + suffix + ":z"
	defer client.Del(busy)
	defer client.Del(quiet)

	var busyHits atomic.Int32
	busyHandler := HandleRequests(client, busy, func(string) error {
		busyHits.Add(1)
		time.Sleep(5 * time.Millisecond)
		return nil
	})
	defer busyHandler.Stop()

	received := make(chan string, 1)
	quietHandler := HandleRequests(client, quiet, func(msg string) error {
		received <- msg
		return nil
	})
	defer quietHandler.Stop()

	for i := 0; i < 200; i++ {
		if err := SendRequest(client, busy, fmt.Sprintf("burst-%d", i)); err != nil {
			t.Fatalf("SendRequest() failed: %v", err)
		}
	}
	if err := SendRequest(client, quiet, "please serve me"); err != nil {
		t.Fatalf("SendRequest() failed: %v", err)
	}

	select {
	case <-received:
	case <-time.After(5 * time.Second):
		t.Errorf("quiet queue starved behind %d busy-queue messages", busyHits.Load())
	}
}

// TestMuxCloseStopsEverything: Close must not hang or leak with both
// muxes active, and the wake key must not be left behind.
func TestMuxCloseStopsEverything(t *testing.T) {
	client, err := New(WithAddress("localhost"), WithCodec(StringCodec{}))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	suffix := time.Now().Format(time.RFC3339Nano)
	hw := client.NewHashWatcher("test:muxclose:" + suffix)
	hw.OnAny(func(field, value string) error { return nil })
	if err := hw.Start(); err != nil {
		t.Fatalf("Start() failed: %v", err)
	}
	HandleRequests(client, "test:muxcloseq:"+suffix, func(string) error { return nil })

	wakeKey := client.queueMux.wakeKey

	done := make(chan error, 1)
	go func() { done <- client.CloseWithTimeout(5 * time.Second) }()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("CloseWithTimeout() = %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("CloseWithTimeout() hung")
	}

	probe, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer probe.Close()

	n, err := probe.Exists(wakeKey)
	if err != nil {
		t.Fatalf("Exists() failed: %v", err)
	}
	if n != 0 {
		t.Errorf("wake key %s left behind after Close", wakeKey)
	}
}

// TestSubscribeAfterCloseFails guards the registration path against a
// client that is already shutting down.
func TestSubscribeAfterCloseFails(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("Close() failed: %v", err)
	}

	hw := client.NewHashWatcher("test:muxclosed:" + time.Now().Format(time.RFC3339Nano))
	hw.OnAny(func(field, value string) error { return nil })
	if err := hw.Start(); err == nil {
		t.Error("Start() on a closed client should fail")
	}
}
