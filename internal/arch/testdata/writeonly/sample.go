// Package writeonly is a FIXTURE for TestTheWriteOnlyDetectorWorksInBothDirections. It lives under
// testdata so the toolchain ignores it, and it exists because a detector with nothing known-bad to
// find passes just as happily when it is broken as when the code is clean.
package writeonly

type sample struct {
	kept     int // written and read
	dropped  int // written and never read — the shape being looked for
	compound int // only ever added to, which is still a write and still never consulted
}

func (s *sample) set(n int) {
	s.kept = n
	s.dropped = n
	s.compound += n
}

func (s *sample) get() int { return s.kept }
