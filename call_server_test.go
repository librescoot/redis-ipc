package redis_ipc

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type pingReq struct {
	Msg string `json:"msg"`
}
type pingResp struct {
	Echo string `json:"echo"`
}

func TestCallServerHappyPath(t *testing.T) {
	client := newTestClient(t, 0)
	defer client.Close()

	ch := uniqueChannel("server-happy")
	srv := NewCallServer(client, ch)
	RegisterCall[addReq, addResp](srv, "add", func(req addReq) (addResp, error) {
		return addResp{Sum: req.A + req.B}, nil
	})
	RegisterCall[pingReq, pingResp](srv, "ping", func(req pingReq) (pingResp, error) {
		return pingResp{Echo: req.Msg}, nil
	})
	srv.Start()
	defer srv.Stop()

	r1, err := CallMethod[addReq, addResp](client, ch, "add", addReq{A: 7, B: 5}, time.Second)
	if err != nil {
		t.Fatalf("add failed: %v", err)
	}
	if r1.Sum != 12 {
		t.Errorf("add Sum = %d, want 12", r1.Sum)
	}

	r2, err := CallMethod[pingReq, pingResp](client, ch, "ping", pingReq{Msg: "hello"}, time.Second)
	if err != nil {
		t.Fatalf("ping failed: %v", err)
	}
	if r2.Echo != "hello" {
		t.Errorf("ping Echo = %q, want %q", r2.Echo, "hello")
	}
}

func TestCallServerUnknownMethod(t *testing.T) {
	client := newTestClient(t, 0)
	defer client.Close()

	ch := uniqueChannel("server-unknown")
	srv := NewCallServer(client, ch)
	RegisterCall[addReq, addResp](srv, "add", func(req addReq) (addResp, error) {
		return addResp{}, nil
	})
	srv.Start()
	defer srv.Stop()

	_, err := CallMethod[addReq, addResp](client, ch, "subtract", addReq{}, time.Second)
	if err == nil {
		t.Fatal("expected error for unknown method")
	}
	if !IsCallError(err) {
		t.Errorf("expected CallError, got %T: %v", err, err)
	}
	if !contains(err.Error(), "unknown method") {
		t.Errorf("expected 'unknown method' in error, got %q", err.Error())
	}
}

func TestCallServerHandlerError(t *testing.T) {
	client := newTestClient(t, 0)
	defer client.Close()

	ch := uniqueChannel("server-error")
	srv := NewCallServer(client, ch)
	RegisterCall[addReq, addResp](srv, "add", func(req addReq) (addResp, error) {
		return addResp{}, errors.New("kaboom")
	})
	srv.Start()
	defer srv.Stop()

	_, err := CallMethod[addReq, addResp](client, ch, "add", addReq{}, time.Second)
	if !IsCallError(err) {
		t.Fatalf("expected CallError, got %T: %v", err, err)
	}
	var ce *CallError
	if errors.As(err, &ce) && ce.Msg != "kaboom" {
		t.Errorf("CallError.Msg = %q, want %q", ce.Msg, "kaboom")
	}
}

func TestCallServerOneBRPOP(t *testing.T) {
	// Verify the server only opens one BRPOP loop regardless of how many
	// methods are registered. Indirect proof: with 4 methods and a
	// pool size of 2 (1 BRPOP + 1 reply publish + headroom), all calls
	// still succeed. Old per-method HandleCalls would have deadlocked.
	client := newTestClient(t, 4)
	defer client.Close()

	ch := uniqueChannel("server-onepool")
	srv := NewCallServer(client, ch)
	for _, m := range []string{"a", "b", "c", "d"} {
		m := m
		RegisterCall[pingReq, pingResp](srv, m, func(req pingReq) (pingResp, error) {
			return pingResp{Echo: m + ":" + req.Msg}, nil
		})
	}
	srv.Start()
	defer srv.Stop()

	for _, m := range []string{"a", "b", "c", "d"} {
		r, err := CallMethod[pingReq, pingResp](client, ch, m, pingReq{Msg: "x"}, 2*time.Second)
		if err != nil {
			t.Fatalf("%s failed: %v", m, err)
		}
		want := m + ":x"
		if r.Echo != want {
			t.Errorf("%s Echo = %q, want %q", m, r.Echo, want)
		}
	}
}

func TestCallServerConcurrentAcrossMethods(t *testing.T) {
	client := newTestClient(t, 32)
	defer client.Close()

	ch := uniqueChannel("server-concurrent")
	srv := NewCallServer(client, ch, WithCallServerConcurrency(8))
	var addCalls, pingCalls atomic.Int32
	RegisterCall[addReq, addResp](srv, "add", func(req addReq) (addResp, error) {
		addCalls.Add(1)
		return addResp{Sum: req.A + req.B}, nil
	})
	RegisterCall[pingReq, pingResp](srv, "ping", func(req pingReq) (pingResp, error) {
		pingCalls.Add(1)
		return pingResp{Echo: req.Msg}, nil
	})
	srv.Start()
	defer srv.Stop()

	const N = 30
	var wg sync.WaitGroup
	errs := make(chan error, 2*N)
	for i := 0; i < N; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			r, err := CallMethod[addReq, addResp](client, ch, "add", addReq{A: i, B: 1}, 5*time.Second)
			if err != nil {
				errs <- err
				return
			}
			if r.Sum != i+1 {
				errs <- fmt.Errorf("add Sum=%d, want %d", r.Sum, i+1)
			}
		}(i)
		go func(i int) {
			defer wg.Done()
			r, err := CallMethod[pingReq, pingResp](client, ch, "ping", pingReq{Msg: fmt.Sprintf("%d", i)}, 5*time.Second)
			if err != nil {
				errs <- err
				return
			}
			want := fmt.Sprintf("%d", i)
			if r.Echo != want {
				errs <- fmt.Errorf("ping Echo=%q, want %q", r.Echo, want)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("call error: %v", err)
	}
	if addCalls.Load() != N {
		t.Errorf("add invoked %d, want %d", addCalls.Load(), N)
	}
	if pingCalls.Load() != N {
		t.Errorf("ping invoked %d, want %d", pingCalls.Load(), N)
	}
}

func TestCallServerStopDrains(t *testing.T) {
	client := newTestClient(t, 0)
	defer client.Close()

	ch := uniqueChannel("server-drain")
	var inflight, completed atomic.Int32
	released := make(chan struct{})
	srv := NewCallServer(client, ch)
	RegisterCall[pingReq, pingResp](srv, "slow", func(req pingReq) (pingResp, error) {
		inflight.Add(1)
		<-released
		completed.Add(1)
		return pingResp{Echo: req.Msg}, nil
	})
	srv.Start()

	doneCh := make(chan error, 1)
	go func() {
		_, err := CallMethod[pingReq, pingResp](client, ch, "slow", pingReq{Msg: "x"}, 5*time.Second)
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

func TestCallServerExpiredRequestDropped(t *testing.T) {
	client := newTestClient(t, 0)
	defer client.Close()

	ch := uniqueChannel("server-expired")

	expired := fmt.Sprintf(
		`{"id":"old","method":"add","reply_channel":"%s:reply:old","deadline":%d,"payload":{"a":1,"b":2}}`,
		ch, time.Now().Add(-time.Second).UnixMilli())
	if _, err := client.LPush(ch, expired); err != nil {
		t.Fatalf("LPush: %v", err)
	}

	var invoked atomic.Bool
	srv := NewCallServer(client, ch)
	RegisterCall[addReq, addResp](srv, "add", func(req addReq) (addResp, error) {
		invoked.Store(true)
		return addResp{Sum: req.A + req.B}, nil
	})
	srv.Start()
	defer srv.Stop()

	time.Sleep(200 * time.Millisecond)
	if invoked.Load() {
		t.Error("handler ran for an expired request")
	}

	r, err := CallMethod[addReq, addResp](client, ch, "add", addReq{A: 4, B: 4}, time.Second)
	if err != nil {
		t.Fatalf("post-drop Call failed: %v", err)
	}
	if r.Sum != 8 {
		t.Errorf("Sum=%d, want 8", r.Sum)
	}
}

func TestRegisterCallAfterStartPanics(t *testing.T) {
	client := newTestClient(t, 0)
	defer client.Close()

	ch := uniqueChannel("server-late-register")
	srv := NewCallServer(client, ch)
	RegisterCall[pingReq, pingResp](srv, "p", func(req pingReq) (pingResp, error) { return pingResp{}, nil })
	srv.Start()
	defer srv.Stop()

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on RegisterCall after Start")
		}
	}()
	RegisterCall[pingReq, pingResp](srv, "q", func(req pingReq) (pingResp, error) { return pingResp{}, nil })
}

func TestRegisterCallDuplicatePanics(t *testing.T) {
	client := newTestClient(t, 0)
	defer client.Close()

	ch := uniqueChannel("server-dup-register")
	srv := NewCallServer(client, ch)
	RegisterCall[pingReq, pingResp](srv, "p", func(req pingReq) (pingResp, error) { return pingResp{}, nil })

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on duplicate RegisterCall")
		}
	}()
	RegisterCall[pingReq, pingResp](srv, "p", func(req pingReq) (pingResp, error) { return pingResp{}, nil })
}
