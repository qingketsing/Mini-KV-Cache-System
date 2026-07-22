package store

import (
	"testing"
	"time"
)

func TestTimingWheelReturnsOnlyDueEvents(t *testing.T) {
	start := time.Unix(100, 0)
	wheel := newTimingWheel(time.Second, start, 8)
	first := expirationEvent{
		key:        "a",
		generation: 1,
		expiresAt:  start.Add(500 * time.Millisecond).UnixNano(),
	}
	later := expirationEvent{
		key:        "b",
		generation: 2,
		expiresAt:  start.Add(10 * time.Second).UnixNano(),
	}
	wheel.schedule(first)
	wheel.schedule(later)

	if got := wheel.advance(start.Add(400 * time.Millisecond)); len(got) != 0 {
		t.Fatalf("early events = %+v", got)
	}
	got := wheel.advance(start.Add(time.Second))
	if len(got) != 1 || got[0].key != "a" {
		t.Fatalf("due events = %+v", got)
	}
	got = wheel.advance(start.Add(10 * time.Second))
	if len(got) != 1 || got[0].key != "b" {
		t.Fatalf("wrapped events = %+v", got)
	}
}

func TestTimingWheelPreservesGenerations(t *testing.T) {
	start := time.Unix(100, 0)
	wheel := newTimingWheel(time.Second, start, 8)
	expiresAt := start.Add(time.Second).UnixNano()
	wheel.schedule(expirationEvent{key: "k", generation: 1, expiresAt: expiresAt})
	wheel.schedule(expirationEvent{key: "k", generation: 2, expiresAt: expiresAt})

	got := wheel.advance(start.Add(time.Second))
	if len(got) != 2 || got[0].generation != 1 || got[1].generation != 2 {
		t.Fatalf("events = %+v", got)
	}
}

func TestTimingWheelDefersCurrentTickExpiration(t *testing.T) {
	start := time.Unix(100, 900*int64(time.Millisecond))
	wheel := newTimingWheel(time.Second, start, 8)
	wheel.schedule(expirationEvent{
		key:        "k",
		generation: 1,
		expiresAt:  start.Add(time.Millisecond).UnixNano(),
	})

	if got := wheel.advance(start.Add(time.Millisecond)); len(got) != 0 {
		t.Fatalf("same-tick events = %+v", got)
	}
	got := wheel.advance(time.Unix(101, 0))
	if len(got) != 1 || got[0].key != "k" {
		t.Fatalf("next-tick events = %+v", got)
	}
}

func TestTimingWheelIgnoresBackwardTime(t *testing.T) {
	start := time.Unix(100, 0)
	wheel := newTimingWheel(time.Second, start, 8)
	wheel.schedule(expirationEvent{
		key:        "k",
		generation: 1,
		expiresAt:  start.Add(time.Second).UnixNano(),
	})

	if got := wheel.advance(start.Add(-time.Second)); len(got) != 0 {
		t.Fatalf("backward events = %+v", got)
	}
	if got := wheel.advance(start.Add(time.Second)); len(got) != 1 {
		t.Fatalf("due events = %+v", got)
	}
}
