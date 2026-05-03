package pipeline

// Phase 1 control read models — deterministic scaffold aligned with TS ingestion-api.

type Stage struct {
	Ordinal int    `json:"ordinal"`
	Key     string `json:"key"`
	Title   string `json:"title"`
}

type Placeholder struct {
	ID          string `json:"id"`
	StageKey    string `json:"stageKey"`
	DisplayName string `json:"displayName"`
	CatalogTier string `json:"catalogTier"`
}

type Summary struct {
	ContractVersion  string        `json:"contractVersion"`
	Stages             []Stage       `json:"stages"`
	AdapterInstances   []Placeholder `json:"adapterInstances"`
}

type CanalSegment struct {
	ID                   string `json:"id"`
	Kind                 string `json:"kind"`
	Label                string `json:"label"`
	FollowsStageOrdinal  int    `json:"followsStageOrdinal"`
	ProviderProfile      string `json:"providerProfile"`
}

type CanalSegments struct {
	Segments []CanalSegment `json:"segments"`
}

var stages = []Stage{
	{Ordinal: 1, Key: "source", Title: "Source"},
	{Ordinal: 2, Key: "source_connector", Title: "Source Connector"},
	{Ordinal: 3, Key: "source_event_buffer", Title: "Source Event Buffer"},
	{Ordinal: 4, Key: "source_canonical_event_serializer", Title: "Source Canonical Event Serializer"},
	{Ordinal: 5, Key: "event_buffer", Title: "Event Buffer"},
	{Ordinal: 6, Key: "sink_event_serializer", Title: "Sink Event Serializer"},
	{Ordinal: 7, Key: "sink_event_buffer", Title: "Sink Event Buffer"},
	{Ordinal: 8, Key: "sink_connector", Title: "Sink Connector"},
}

var placeholders = []Placeholder{
	{
		ID:          "adapter.placeholder.source_connector",
		StageKey:    "source_connector",
		DisplayName: "Source connector (placeholder)",
		CatalogTier: "tier-1",
	},
	{
		ID:          "adapter.placeholder.sink_connector",
		StageKey:    "sink_connector",
		DisplayName: "Sink connector (placeholder)",
		CatalogTier: "tier-2",
	},
	{
		ID:          "adapter.placeholder.community_sink",
		StageKey:    "sink_connector",
		DisplayName: "Community sink adapter (placeholder)",
		CatalogTier: "tier-3",
	},
}

var canalSegments = []CanalSegment{
	{
		ID:                  "canal.segment.source_event_buffer",
		Kind:                "buffer",
		Label:               "Source Event Buffer",
		FollowsStageOrdinal: 2,
		ProviderProfile:     "p1-local",
	},
	{
		ID:                  "canal.segment.event_buffer",
		Kind:                "buffer",
		Label:               "Event Buffer",
		FollowsStageOrdinal: 4,
		ProviderProfile:     "p1-local",
	},
	{
		ID:                  "canal.segment.sink_event_buffer",
		Kind:                "buffer",
		Label:               "Sink Event Buffer",
		FollowsStageOrdinal: 6,
		ProviderProfile:     "p1-local",
	},
}

func PipelineSummary() Summary {
	out := make([]Stage, len(stages))
	copy(out, stages)
	ph := make([]Placeholder, len(placeholders))
	copy(ph, placeholders)
	return Summary{
		ContractVersion: "0.1.0",
		Stages:          out,
		AdapterInstances: ph,
	}
}

func CanalSegmentsRead() CanalSegments {
	segs := make([]CanalSegment, len(canalSegments))
	copy(segs, canalSegments)
	return CanalSegments{Segments: segs}
}

func PlaceholderByCatalogID(id string) *Placeholder {
	for i := range placeholders {
		if placeholders[i].ID == id {
			p := placeholders[i]
			return &p
		}
	}
	return nil
}
