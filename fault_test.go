package redis_ipc

import (
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"
)

func newTestFaultReporter(t *testing.T) (*Client, *FaultReporter, string, string, string) {
	t.Helper()
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	suffix := time.Now().Format(time.RFC3339Nano)
	group := "test:fr:" + suffix
	stream := "test:fr-stream:" + suffix
	setKey := group + ":fault"
	fr := client.NewFaultReporter(group, WithFaultStream(stream))
	return client, fr, group, setKey, stream
}

func cleanupFault(client *Client, setKey, stream string) {
	client.redis.Del(client.Context(), setKey, stream)
}

func TestFaultReporterRaiseAndClear(t *testing.T) {
	client, fr, group, setKey, stream := newTestFaultReporter(t)
	defer client.Close()
	defer cleanupFault(client, setKey, stream)

	if err := fr.Raise(42, "oh no"); err != nil {
		t.Fatalf("Raise(42): %v", err)
	}

	has, err := fr.Has(42)
	if err != nil {
		t.Fatalf("Has(42): %v", err)
	}
	if !has {
		t.Error("Has(42) = false after Raise; want true")
	}

	all, err := fr.All()
	if err != nil {
		t.Fatalf("All(): %v", err)
	}
	if len(all) != 1 || all[0] != 42 {
		t.Errorf("All() = %v; want [42]", all)
	}

	msgs, err := client.redis.XRange(client.Context(), stream, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("stream entries after Raise = %d; want 1", len(msgs))
	}
	if got := msgs[0].Values["group"]; got != group {
		t.Errorf("group = %v; want %s", got, group)
	}
	if got := msgs[0].Values["code"]; got != "42" {
		t.Errorf("code = %v; want 42", got)
	}
	if got := msgs[0].Values["description"]; got != "oh no" {
		t.Errorf("description = %v; want \"oh no\"", got)
	}

	if err := fr.Clear(42); err != nil {
		t.Fatalf("Clear(42): %v", err)
	}

	has, err = fr.Has(42)
	if err != nil {
		t.Fatalf("Has(42) after Clear: %v", err)
	}
	if has {
		t.Error("Has(42) = true after Clear; want false")
	}

	msgs, err = client.redis.XRange(client.Context(), stream, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("stream entries after Clear = %d; want 2", len(msgs))
	}
	if got := msgs[1].Values["code"]; got != "-42" {
		t.Errorf("clear code = %v; want -42", got)
	}
	if _, hasDesc := msgs[1].Values["description"]; hasDesc {
		t.Errorf("clear entry should not include description; got %v", msgs[1].Values)
	}
}

func TestFaultReporterRaiseIdempotent(t *testing.T) {
	client, fr, _, setKey, stream := newTestFaultReporter(t)
	defer client.Close()
	defer cleanupFault(client, setKey, stream)

	for i := 0; i < 3; i++ {
		if err := fr.Raise(7, "nope"); err != nil {
			t.Fatalf("Raise(7) iter %d: %v", i, err)
		}
	}

	msgs, err := client.redis.XRange(client.Context(), stream, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange: %v", err)
	}
	if len(msgs) != 1 {
		t.Errorf("stream entries after 3 Raise(7) = %d; want 1 (idempotent)", len(msgs))
	}
}

func TestFaultReporterClearIdempotent(t *testing.T) {
	client, fr, _, setKey, stream := newTestFaultReporter(t)
	defer client.Close()
	defer cleanupFault(client, setKey, stream)

	for i := 0; i < 3; i++ {
		if err := fr.Clear(99); err != nil {
			t.Fatalf("Clear(99) iter %d: %v", i, err)
		}
	}

	msgs, err := client.redis.XRange(client.Context(), stream, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange: %v", err)
	}
	if len(msgs) != 0 {
		t.Errorf("stream entries after Clear of unset code = %d; want 0", len(msgs))
	}
}

func TestFaultReporterPubSubFires(t *testing.T) {
	client, fr, group, setKey, stream := newTestFaultReporter(t)
	defer client.Close()
	defer cleanupFault(client, setKey, stream)

	pubsub := client.redis.Subscribe(client.Context(), group)
	defer pubsub.Close()
	if _, err := pubsub.Receive(client.Context()); err != nil {
		t.Fatalf("subscribe receive: %v", err)
	}
	ch := pubsub.Channel()

	if err := fr.Raise(1, "hi"); err != nil {
		t.Fatalf("Raise: %v", err)
	}
	select {
	case msg := <-ch:
		if msg.Payload != "fault" {
			t.Errorf("payload = %s; want fault", msg.Payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no pubsub message received after Raise")
	}

	if err := fr.Raise(1, "hi again"); err != nil {
		t.Fatalf("Raise (idempotent): %v", err)
	}
	select {
	case msg := <-ch:
		t.Errorf("unexpected pubsub message after idempotent Raise: %v", msg)
	case <-time.After(200 * time.Millisecond):
	}

	if err := fr.Clear(1); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	select {
	case msg := <-ch:
		if msg.Payload != "fault" {
			t.Errorf("payload = %s; want fault", msg.Payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no pubsub message received after Clear")
	}
}

func TestFaultReporterAllAndHas(t *testing.T) {
	client, fr, _, setKey, stream := newTestFaultReporter(t)
	defer client.Close()
	defer cleanupFault(client, setKey, stream)

	for _, c := range []int{3, 1, 2} {
		if err := fr.Raise(c, "x"); err != nil {
			t.Fatalf("Raise(%d): %v", c, err)
		}
	}

	codes, err := fr.All()
	if err != nil {
		t.Fatalf("All(): %v", err)
	}
	sort.Ints(codes)
	if len(codes) != 3 || codes[0] != 1 || codes[1] != 2 || codes[2] != 3 {
		t.Errorf("All() = %v; want [1 2 3]", codes)
	}

	for _, c := range []int{1, 2, 3} {
		has, err := fr.Has(c)
		if err != nil {
			t.Fatalf("Has(%d): %v", c, err)
		}
		if !has {
			t.Errorf("Has(%d) = false; want true", c)
		}
	}

	has, err := fr.Has(99)
	if err != nil {
		t.Fatalf("Has(99): %v", err)
	}
	if has {
		t.Error("Has(99) = true; want false")
	}
}

func TestFaultReporterDefaults(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer client.Close()

	suffix := time.Now().Format(time.RFC3339Nano)
	group := "test:fr-defaults:" + suffix
	defer cleanupFault(client, group+":fault", "events:faults")

	fr := client.NewFaultReporter(group)
	if fr.Group() != group {
		t.Errorf("Group() = %s; want %s", fr.Group(), group)
	}
	if err := fr.Raise(5, "default test"); err != nil {
		t.Fatalf("Raise: %v", err)
	}

	exists, err := client.redis.SIsMember(client.Context(), group+":fault", 5).Result()
	if err != nil {
		t.Fatalf("SIsMember: %v", err)
	}
	if !exists {
		t.Errorf("default set key %s:fault did not get the code", group)
	}

	pubsub := client.redis.Subscribe(client.Context(), group)
	defer pubsub.Close()
	if _, err := pubsub.Receive(client.Context()); err != nil {
		t.Fatalf("subscribe receive: %v", err)
	}
	ch := pubsub.Channel()

	if err := fr.Clear(5); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	select {
	case msg := <-ch:
		if msg.Channel != group {
			t.Errorf("default channel = %s; want %s", msg.Channel, group)
		}
		if msg.Payload != "fault" {
			t.Errorf("default payload = %s; want fault", msg.Payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no pubsub on default channel after Clear")
	}
}

func TestFaultReporterCustomOptions(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer client.Close()

	suffix := time.Now().Format(time.RFC3339Nano)
	customSet := "custom:set:" + suffix
	customStream := "custom:stream:" + suffix
	customChan := "custom:chan:" + suffix
	defer cleanupFault(client, customSet, customStream)

	fr := client.NewFaultReporter("ignored",
		WithFaultSetKey(customSet),
		WithFaultStream(customStream),
		WithFaultChannel(customChan),
		WithFaultPayload("boom"),
		WithFaultMaxLen(50),
	)

	pubsub := client.redis.Subscribe(client.Context(), customChan)
	defer pubsub.Close()
	if _, err := pubsub.Receive(client.Context()); err != nil {
		t.Fatalf("subscribe receive: %v", err)
	}
	ch := pubsub.Channel()

	if err := fr.Raise(11, "custom"); err != nil {
		t.Fatalf("Raise: %v", err)
	}

	select {
	case msg := <-ch:
		if msg.Channel != customChan || msg.Payload != "boom" {
			t.Errorf("got channel=%s payload=%s; want %s/boom", msg.Channel, msg.Payload, customChan)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no pubsub on custom channel")
	}

	if exists, err := client.redis.SIsMember(client.Context(), customSet, 11).Result(); err != nil {
		t.Fatalf("SIsMember: %v", err)
	} else if !exists {
		t.Error("code did not land in custom set key")
	}

	msgs, err := client.redis.XRange(client.Context(), customStream, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange: %v", err)
	}
	if len(msgs) != 1 {
		t.Errorf("custom stream entries = %d; want 1", len(msgs))
	}
}

func TestFaultReporterMultiGroupIsolation(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer client.Close()

	suffix := time.Now().Format(time.RFC3339Nano)
	streamA := "test:fr-iso-a:" + suffix
	streamB := "test:fr-iso-b:" + suffix
	groupA := "test:fr-iso-a:" + suffix
	groupB := "test:fr-iso-b:" + suffix
	defer cleanupFault(client, groupA+":fault", streamA)
	defer cleanupFault(client, groupB+":fault", streamB)

	frA := client.NewFaultReporter(groupA, WithFaultStream(streamA))
	frB := client.NewFaultReporter(groupB, WithFaultStream(streamB))

	if err := frA.Raise(100, "a"); err != nil {
		t.Fatalf("A.Raise: %v", err)
	}
	if err := frB.Raise(200, "b"); err != nil {
		t.Fatalf("B.Raise: %v", err)
	}

	if has, _ := frA.Has(200); has {
		t.Error("A should not see B's code 200")
	}
	if has, _ := frB.Has(100); has {
		t.Error("B should not see A's code 100")
	}

	codesA, _ := frA.All()
	codesB, _ := frB.All()
	if len(codesA) != 1 || codesA[0] != 100 {
		t.Errorf("A.All() = %v; want [100]", codesA)
	}
	if len(codesB) != 1 || codesB[0] != 200 {
		t.Errorf("B.All() = %v; want [200]", codesB)
	}
}

func TestFaultReporterAllNonInteger(t *testing.T) {
	client, err := New(WithAddress("localhost"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer client.Close()

	suffix := time.Now().Format(time.RFC3339Nano)
	group := "test:fr-noninteger:" + suffix
	setKey := group + ":fault"
	defer client.redis.Del(client.Context(), setKey)

	if _, err := client.redis.SAdd(client.Context(), setKey, "not-a-number").Result(); err != nil {
		t.Fatalf("SAdd: %v", err)
	}
	fr := client.NewFaultReporter(group)
	if _, err := fr.All(); err == nil {
		t.Error("All() should fail on non-integer member")
	}
}

func TestFaultReporterConcurrentRaiseClear(t *testing.T) {
	client, fr, _, setKey, stream := newTestFaultReporter(t)
	defer client.Close()
	defer cleanupFault(client, setKey, stream)

	const N = 50
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(2)
		go func(code int) {
			defer wg.Done()
			fr.Raise(code, "c"+strconv.Itoa(code))
		}(i)
		go func(code int) {
			defer wg.Done()
			fr.Clear(code)
		}(i)
	}
	wg.Wait()

	codes, err := fr.All()
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	for _, c := range codes {
		has, _ := fr.Has(c)
		if !has {
			t.Errorf("Has(%d) = false but it's in All()", c)
		}
	}

	msgs, err := client.redis.XRange(client.Context(), stream, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange: %v", err)
	}
	t.Logf("after %d Raise+Clear pairs: set=%d stream=%d", N, len(codes), len(msgs))
}
