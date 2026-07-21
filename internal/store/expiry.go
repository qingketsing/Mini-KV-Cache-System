package store

import (
	"sync"
	"time"
)

const defaultTimingWheelSlots = 512

type expirationEvent struct {
	shardID    uint32
	key        string
	generation uint64
	expiresAt  int64
	dueTick    int64
}

type timingWheel struct {
	mu sync.Mutex

	resolution  int64
	currentTick int64
	slots       [][]expirationEvent
}

func newTimingWheel(resolution time.Duration, start time.Time, slotCount int) *timingWheel {
	if resolution <= 0 {
		panic("store: timing wheel resolution must be positive")
	}
	if slotCount <= 0 {
		panic("store: timing wheel slot count must be positive")
	}
	resolutionNanos := int64(resolution)
	return &timingWheel{
		resolution:  resolutionNanos,
		currentTick: start.UnixNano() / resolutionNanos,
		slots:       make([][]expirationEvent, slotCount),
	}
}

func (w *timingWheel) schedule(event expirationEvent) {
	w.mu.Lock()
	dueTick := ceilingTick(event.expiresAt, w.resolution)
	if dueTick <= w.currentTick {
		dueTick = w.currentTick + 1
	}
	event.dueTick = dueTick
	index := wheelSlot(dueTick, len(w.slots))
	w.slots[index] = append(w.slots[index], event)
	w.mu.Unlock()
}

func (w *timingWheel) advance(now time.Time) []expirationEvent {
	nowNanos := now.UnixNano()
	targetTick := nowNanos / w.resolution

	w.mu.Lock()
	defer w.mu.Unlock()
	if targetTick <= w.currentTick {
		return nil
	}

	var due []expirationEvent
	for w.currentTick < targetTick {
		w.currentTick++
		index := wheelSlot(w.currentTick, len(w.slots))
		events := w.slots[index]
		w.slots[index] = nil
		for _, event := range events {
			if event.dueTick <= w.currentTick && event.expiresAt <= nowNanos {
				due = append(due, event)
				continue
			}
			w.slots[index] = append(w.slots[index], event)
		}
	}
	return due
}

func ceilingTick(timestamp, resolution int64) int64 {
	tick := timestamp / resolution
	if timestamp%resolution != 0 {
		tick++
	}
	return tick
}

func wheelSlot(tick int64, slotCount int) int {
	index := tick % int64(slotCount)
	if index < 0 {
		index += int64(slotCount)
	}
	return int(index)
}
