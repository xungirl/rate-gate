package breaker

import (
	"testing"
	"time"

	"github.com/xungirl/rate-gate/internal/clock"
)

func TestBreakerLifecycle(t *testing.T) {
	fc := clock.NewFake(time.Unix(0, 0))
	var changes []State
	b := New(3, time.Second, fc, func(s State) { changes = append(changes, s) })

	// Two failures: still closed.
	for i := 0; i < 2; i++ {
		if !b.Allow() {
			t.Fatal("closed breaker rejected a call")
		}
		b.Failure()
	}
	if b.State() != Closed {
		t.Fatalf("state=%s after 2 failures, want closed", b.State())
	}
	// A success resets the consecutive-failure count.
	b.Allow()
	b.Success()
	for i := 0; i < 2; i++ {
		b.Allow()
		b.Failure()
	}
	if b.State() != Closed {
		t.Fatal("success did not reset the failure count")
	}
	// Third consecutive failure trips it.
	b.Allow()
	b.Failure()
	if b.State() != Open {
		t.Fatalf("state=%s, want open", b.State())
	}
	if b.Allow() {
		t.Fatal("open breaker allowed a call before the timeout")
	}

	// After the timeout exactly one probe is allowed.
	fc.Advance(time.Second)
	if !b.Allow() {
		t.Fatal("no probe allowed after timeout")
	}
	if b.State() != HalfOpen {
		t.Fatalf("state=%s, want half_open", b.State())
	}
	if b.Allow() {
		t.Fatal("second concurrent probe allowed")
	}

	// Failed probe: open again, timer restarts.
	b.Failure()
	if b.State() != Open || b.Allow() {
		t.Fatal("failed probe should re-open the breaker")
	}
	fc.Advance(time.Second)
	b.Allow()
	b.Success()
	if b.State() != Closed {
		t.Fatalf("state=%s after successful probe, want closed", b.State())
	}

	want := []State{Open, HalfOpen, Open, HalfOpen, Closed}
	if len(changes) != len(want) {
		t.Fatalf("state changes %v, want %v", changes, want)
	}
	for i := range want {
		if changes[i] != want[i] {
			t.Fatalf("state changes %v, want %v", changes, want)
		}
	}
}

func TestDiscardFreesProbe(t *testing.T) {
	fc := clock.NewFake(time.Unix(0, 0))
	b := New(1, time.Second, fc, nil)
	b.Allow()
	b.Failure()
	fc.Advance(time.Second)
	b.Allow() // probe
	b.Discard()
	if !b.Allow() {
		t.Fatal("probe slot not freed by Discard")
	}
}
