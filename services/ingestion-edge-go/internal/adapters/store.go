package adapters

import (
	"strings"
	"time"

	"canal.ingestion-edge-go/internal/pipeline"
)

type Record struct {
	ID               string  `json:"id"`
	CatalogAdapterID string  `json:"catalogAdapterId"`
	CatalogTier      string  `json:"catalogTier"`
	StageKey         string  `json:"stageKey"`
	DisplayName      string  `json:"displayName"`
	OperatorLabel    *string `json:"operatorLabel"`
	CreatedAt        string  `json:"createdAt"`
	UpdatedAt        string  `json:"updatedAt"`
}

type Store struct {
	byID map[string]*Record
}

func NewStore() *Store {
	return &Store{byID: make(map[string]*Record)}
}

func (s *Store) List() []Record {
	out := make([]Record, 0, len(s.byID))
	for _, r := range s.byID {
		out = append(out, *r)
	}
	// stable sort by createdAt like TS localeCompare
	for i := 0; i < len(out); i++ {
		for j := i + 1; j < len(out); j++ {
			if out[j].CreatedAt < out[i].CreatedAt {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

func (s *Store) Get(id string) (*Record, bool) {
	r, ok := s.byID[id]
	if !ok {
		return nil, false
	}
	cp := *r
	return &cp, true
}

func (s *Store) Create(catalogAdapterID string, operatorLabel *string) (*Record, string) {
	ph := pipeline.PlaceholderByCatalogID(catalogAdapterID)
	if ph == nil {
		return nil, "`catalogAdapterId` is not a known pipeline adapter placeholder — tier is taken only from the control read model catalog."
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	var label *string
	if operatorLabel != nil {
		t := strings.TrimSpace(*operatorLabel)
		if t != "" {
			label = &t
		}
	}
	id := newUUID()
	rec := &Record{
		ID:               id,
		CatalogAdapterID: ph.ID,
		CatalogTier:      ph.CatalogTier,
		StageKey:         ph.StageKey,
		DisplayName:      ph.DisplayName,
		OperatorLabel:    label,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	s.byID[id] = rec
	cp := *rec
	return &cp, ""
}

// LabelPatch: Nil means JSON null (clear); string pointer means set trimmed; absent = field not in request (use separate flags from handler).
type LabelPatch struct {
	Defined bool
	Value   *string
}

func (s *Store) Patch(id string, catalogAdapterID *string, label LabelPatch) (*Record, string, bool) {
	rec, ok := s.byID[id]
	if !ok {
		return nil, "", false
	}
	if catalogAdapterID != nil {
		ph := pipeline.PlaceholderByCatalogID(*catalogAdapterID)
		if ph == nil {
			return nil, "`catalogAdapterId` is not a known pipeline adapter placeholder — tier is taken only from the control read model catalog.", true
		}
		rec.CatalogAdapterID = ph.ID
		rec.CatalogTier = ph.CatalogTier
		rec.StageKey = ph.StageKey
		rec.DisplayName = ph.DisplayName
	}
	if label.Defined {
		if label.Value == nil {
			rec.OperatorLabel = nil
		} else {
			t := strings.TrimSpace(*label.Value)
			if t == "" {
				rec.OperatorLabel = nil
			} else {
				rec.OperatorLabel = &t
			}
		}
	}
	rec.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	cp := *rec
	return &cp, "", true
}

func (s *Store) Delete(id string) bool {
	_, ok := s.byID[id]
	if !ok {
		return false
	}
	delete(s.byID, id)
	return true
}
