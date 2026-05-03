package dedupe

import "sync"

const maxSeen = 50_000

// Seen tracks global event id dedupe with overflow pruning like TS ingestion route.
type Seen struct {
	mu    sync.Mutex
	ids   map[string]struct{}
	order []string
}

func New() *Seen {
	return &Seen{ids: make(map[string]struct{})}
}

// Check returns true if id was already seen (duplicate). Otherwise records id.
func (s *Seen) Check(id string) (duplicate bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.ids[id]; ok {
		return true
	}
	s.ids[id] = struct{}{}
	s.order = append(s.order, id)
	s.pruneLocked()
	return false
}

func (s *Seen) pruneLocked() {
	if len(s.ids) <= maxSeen {
		return
	}
	overflow := len(s.ids) - maxSeen
	for i := 0; i < overflow && len(s.order) > 0; i++ {
		oldest := s.order[0]
		s.order = s.order[1:]
		delete(s.ids, oldest)
	}
}
