package redis_ipc

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type addReq struct {
	A int `json:"a"`
	B int `json:"b"`
}
type addResp struct {
	Sum int `json:"sum"`
}

func uniqueChannel(prefix string) string {
	return fmt.Sprintf("test:%s:%d", prefix, time.Now().UnixNano())
}

func newTestClient(t *testing.T, poolSize int) *Client {
	t.Helper()
	opts := []Option{WithAddress("localhost")}
	if poolSize > 0 {
		opts = append(opts, WithPoolSize(poolSize))
	}
	c, err := New(opts...)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	return c
}

func TestCallHappyPath(t *testing.T) {
	client := newTestClient(t, 0)
	defer client.Close()

	ch := uniqueChannel("call-happy")
	srv := HandleCalls[addReq, addResp](client, ch, func(req addReq) (addResp, error) {
		return addResp{Sum: req.A + req.B}, nil
	})
	defer srv.Stop()

	resp, err := Call[addReq, addResp](client, ch, addReq{A: 2, B: 3}, time.Second)
	if err != nil {
		t.Fatalf("Call failed: %v", err)
	}
	if resp.Sum != 5 {
		t.Errorf("Sum = %d, want 5", resp.Sum)
	}
}

func TestCallTimeoutNoServer(t *testing.T) {
	client := newTestClient(t, 0)
	defer client.Close()

	ch := uniqueChannel("call-timeout")
	defer client.Del(ch)

	start := time.Now()
	_, err := Call[addReq, addResp](client, ch, addReq{A: 1, B: 1}, 200*time.Millisecond)
	elapsed := time.Since(start)

	if !errors.Is(err, ErrCallTimeout) {
		t.Errorf("expected ErrCallTimeout, got %v", err)
	}
	if elapsed < 150*time.Millisecond || elapsed > 500*time.Millisecond {
		t.Errorf("Call returned in %v, expected ~200ms", elapsed)
	}
}

func TestCallServerError(t *testing.T) {
	client := newTestClient(t, 0)
	defer client.Close()

	ch := uniqueChannel("call-error")
	srv := HandleCalls[addReq, addResp](client, ch, func(req addReq) (addResp, error) {
		return addResp{}, errors.New("nope")
	})
	defer srv.Stop()

	_, err := Call[addReq, addResp](client, ch, addReq{}, time.Second)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !IsCallError(err) {
		t.Errorf("expected CallError, got %T: %v", err, err)
	}
	var ce *CallError
	if errors.As(err, &ce) && ce.Msg != "nope" {
		t.Errorf("CallError.Msg = %q, want %q", ce.Msg, "nope")
	}
}

func TestCallMalformedRequest(t *testing.T) {
	client := newTestClient(t, 0)
	defer client.Close()

	ch := uniqueChannel("call-malformed")
	srv := HandleCalls[addReq, addResp](client, ch, func(req addReq) (addResp, error) {
		return addResp{Sum: 99}, nil
	})
	defer srv.Stop()

	// Subscribe to the reply channel so the test can verify what the
	// server sends back.
	replyCh := ch + ":reply:bad"
	pubsub := client.Raw().Subscribe(client.Context(), replyCh)
	defer pubsub.Close()
	if _, err := pubsub.Receive(client.Context()); err != nil {
		t.Fatalf("subscribe confirm: %v", err)
	}

	bad := fmt.Sprintf(
		`{"id":"x","reply_channel":%q,"deadline":%d,"payload":"not-an-object"}`,
		replyCh, time.Now().Add(time.Second).UnixMilli())
	if _, err := client.LPush(ch, bad); err != nil {
		t.Fatalf("LPush bad envelope: %v", err)
	}

	msg, err := pubsub.ReceiveMessage(client.Context())
	if err != nil {
		t.Fatalf("recv reply: %v", err)
	}
	if !contains(msg.Payload, `"ok":false`) || !contains(msg.Payload, "decode request") {
		t.Errorf("expected ok=false + decode-request error, got %q", msg.Payload)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func TestCallServerCrashMidCall(t *testing.T) {
	client := newTestClient(t, 0)
	defer client.Close()

	ch := uniqueChannel("call-servercrash")

	started := make(chan struct{})
	srv := HandleCalls[addReq, addResp](client, ch, func(req addReq) (addResp, error) {
		close(started)
		// Simulates a slow handler that finishes well after the caller's
		// deadline. The reply (when it eventually publishes) lands on
		// a channel with no subscriber and is dropped.
		time.Sleep(800 * time.Millisecond)
		return addResp{Sum: req.A + req.B}, nil
	})
	defer srv.Stop()

	doneCh := make(chan error, 1)
	go func() {
		_, err := Call[addReq, addResp](client, ch, addReq{A: 1, B: 1}, 200*time.Millisecond)
		doneCh <- err
	}()
	<-started

	select {
	case err := <-doneCh:
		if !errors.Is(err, ErrCallTimeout) {
			t.Errorf("expected ErrCallTimeout, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Call did not return within 1s")
	}
}

func TestCallConcurrentCallers(t *testing.T) {
	client := newTestClient(t, 64)
	defer client.Close()

	ch := uniqueChannel("call-concurrent")
	var seen atomic.Int32
	srv := HandleCalls[addReq, addResp](client, ch, func(req addReq) (addResp, error) {
		seen.Add(1)
		return addResp{Sum: req.A + req.B}, nil
	}, WithCallConcurrency(8))
	defer srv.Stop()

	const N = 50
	var wg sync.WaitGroup
	errs := make(chan error, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp, err := Call[addReq, addResp](client, ch, addReq{A: i, B: i}, 5*time.Second)
			if err != nil {
				errs <- err
				return
			}
			if resp.Sum != 2*i {
				errs <- fmt.Errorf("Sum=%d, want %d", resp.Sum, 2*i)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("call error: %v", err)
	}
	if seen.Load() != N {
		t.Errorf("handler invoked %d times, want %d", seen.Load(), N)
	}
}

func TestCallShortTimeout(t *testing.T) {
	// Sub-second timeouts must work; pub/sub-based reply doesn't suffer
	// the BLPop 1s floor in go-redis v9.
	client := newTestClient(t, 0)
	defer client.Close()

	ch := uniqueChannel("call-short")
	defer client.Del(ch)

	start := time.Now()
	_, err := Call[addReq, addResp](client, ch, addReq{}, 50*time.Millisecond)
	elapsed := time.Since(start)
	if !errors.Is(err, ErrCallTimeout) {
		t.Errorf("expected ErrCallTimeout, got %v", err)
	}
	if elapsed > 250*time.Millisecond {
		t.Errorf("50ms timeout took %v", elapsed)
	}
}

func TestCallHandlerStopDrains(t *testing.T) {
	client := newTestClient(t, 0)
	defer client.Close()

	ch := uniqueChannel("call-stopdrain")
	var inflight, completed atomic.Int32
	released := make(chan struct{})
	srv := HandleCalls[addReq, addResp](client, ch, func(req addReq) (addResp, error) {
		inflight.Add(1)
		<-released
		completed.Add(1)
		return addResp{Sum: req.A + req.B}, nil
	})

	doneCh := make(chan error, 1)
	go func() {
		_, err := Call[addReq, addResp](client, ch, addReq{A: 1, B: 2}, 5*time.Second)
		doneCh <- err
	}()
	deadline := time.After(time.Second)
	for inflight.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("handler did not start")
		case <-time.After(10 * time.Millisecond):
		}
	}

	stopReturned := make(chan struct{})
	go func() { srv.Stop(); close(stopReturned) }()
	select {
	case <-stopReturned:
		t.Fatal("Stop returned before handler finished")
	case <-time.After(50 * time.Millisecond):
	}

	close(released)
	select {
	case <-stopReturned:
	case <-time.After(time.Second):
		t.Fatal("Stop did not return after handler finished")
	}
	if completed.Load() != 1 {
		t.Errorf("handler did not complete (completed=%d)", completed.Load())
	}
	<-doneCh
}

func TestCallExpiredRequestDropped(t *testing.T) {
	client := newTestClient(t, 0)
	defer client.Close()

	ch := uniqueChannel("call-expired")

	expired := fmt.Sprintf(
		`{"id":"old","reply_channel":"%s:reply:old","deadline":%d,"payload":{"a":1,"b":2}}`,
		ch, time.Now().Add(-time.Second).UnixMilli())
	if _, err := client.LPush(ch, expired); err != nil {
		t.Fatalf("LPush: %v", err)
	}

	var invoked atomic.Bool
	srv := HandleCalls[addReq, addResp](client, ch, func(req addReq) (addResp, error) {
		invoked.Store(true)
		return addResp{Sum: req.A + req.B}, nil
	})
	defer srv.Stop()

	time.Sleep(200 * time.Millisecond)
	if invoked.Load() {
		t.Error("handler ran for an expired request")
	}

	resp, err := Call[addReq, addResp](client, ch, addReq{A: 4, B: 4}, time.Second)
	if err != nil {
		t.Fatalf("post-drop Call failed: %v", err)
	}
	if resp.Sum != 8 {
		t.Errorf("Sum=%d, want 8", resp.Sum)
	}
}
