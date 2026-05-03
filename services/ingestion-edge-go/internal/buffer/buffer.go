package buffer

import (
	"encoding/json"
	"sync"
	"time"
)

// IngestEvent matches OpenAPI IngestEvent (payload optional, arbitrary JSON).
type IngestEvent struct {
	ID         string          `json:"id"`
	Type       string          `json:"type"`
	OccurredAt string          `json:"occurredAt"`
	Payload    json.RawMessage `json:"payload,omitempty"`
}

type Record struct {
	Sequence     int         `json:"sequence"`
	Source       string      `json:"source"`
	EnqueuedAtMs int64       `json:"enqueuedAtMs"`
	Event        IngestEvent `json:"event"`
}

// P1LocalEventBuffer holds accepted Phase 1 batch events in memory (scaffold).
type P1LocalEventBuffer struct {
	mu           sync.Mutex
	nextSequence int
	queue        []Record
}

func NewP1Local() *P1LocalEventBuffer {
	return &P1LocalEventBuffer{nextSequence: 1}
}

func (b *P1LocalEventBuffer) Append(source string, events []IngestEvent) []int {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now().UnixMilli()
	assigned := make([]int, 0, len(events))
	for _, event := range events {
		seq := b.nextSequence
		b.nextSequence++
		b.queue = append(b.queue, Record{
			Sequence:     seq,
			Source:       source,
			EnqueuedAtMs: now,
			Event:        event,
		})
		assigned = append(assigned, seq)
	}
	return assigned
}

func (b *P1LocalEventBuffer) ReadAfter(cursorSequence int, limit int) []Record {
	if limit < 1 {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []Record
	for _, rec := range b.queue {
		if rec.Sequence <= cursorSequence {
			continue
		}
		out = append(out, rec)
		if len(out) >= limit {
			break
		}
	}
	return out
}
