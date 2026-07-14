package gateway

import (
	"encoding/json"
	"io"
	"net/http"
	"time"
)

type Operation string

const (
	OpChatCompletions Operation = "chat.completions"
	OpResponses       Operation = "responses"
	OpCompletions     Operation = "completions"
	OpEmbeddings      Operation = "embeddings"
)

type Subject struct {
	KeyID string
}

type ScopeKind string

const (
	ScopeKey ScopeKind = "key"
)

type ScopeRef struct {
	Kind ScopeKind
	ID   string
}

type EstimatedUsage struct {
	InputTokens          int64
	ReservedOutputTokens int64
	EstimatedSpendMicros int64
}

type ActualUsage struct {
	InputTokens  int64
	OutputTokens int64
	SpendMicros  int64
}

type QuotaUsage struct {
	ActiveRequests  int64 `json:"active_requests,omitempty"`
	RPMCurrent      int64 `json:"rpm_current,omitempty"`
	TPMCurrent      int64 `json:"tpm_current,omitempty"`
	SpendUsedMicros int64 `json:"spend_used_micros,omitempty"`
	SpendHeldMicros int64 `json:"spend_held_micros,omitempty"`
	DailyUsedTokens int64 `json:"daily_used_tokens,omitempty"`
	DailyHeldTokens int64 `json:"daily_held_tokens,omitempty"`
	MonthUsedTokens int64 `json:"monthly_used_tokens,omitempty"`
	MonthHeldTokens int64 `json:"monthly_held_tokens,omitempty"`
}

type ScopedLimit struct {
	Ref    ScopeRef
	Limits LimitSpec
}

type QuotaTicket struct {
	RequestID string
	Scopes    []ScopeRef
}

type Meta struct {
	RequestID   string
	ExecutionID string
	Principal   string
	User        string
	Project     string
	RemoteAddr  string
	BodyBytes   int64
	Headers     http.Header
	ReceivedAt  time.Time
}

type RequestHints struct {
	Metadata                 map[string]string
	User                     string
	MaxOutputTokens          int
	OutputMultiplicity       int64
	PromptText               string
	EstimatedInputTokens     int64
	VisionInputParts         int64
	AudioInputParts          int64
	RequiresTools            bool
	RequiresStructuredOutput bool
	RequiresVision           bool
	RequiresAudio            bool
	RequiresReasoning        bool
	ProviderUnits            []string
	MayWritePromptCache      bool
	APIVersion               string
}

type Request struct {
	Meta      Meta
	Provider  string
	Operation Operation
	Model     string
	Stream    bool
	RawBody   json.RawMessage
	Hints     RequestHints
}

// TokenProjector removes provider-native binary payloads from the textual
// representation used for token estimation. Implementations belong to
// protocol adapters; policy remains provider-agnostic.
type TokenProjector interface {
	ProjectTokenText(json.RawMessage) string
}

type Usage struct {
	InputTokens      int64            `json:"prompt_tokens,omitempty"`
	OutputTokens     int64            `json:"completion_tokens,omitempty"`
	TotalTokens      int64            `json:"total_tokens,omitempty"`
	CacheReadTokens  int64            `json:"cache_read_input_tokens,omitempty"`
	CacheWriteTokens int64            `json:"cache_creation_input_tokens,omitempty"`
	InputDetails     *UsageDetails    `json:"input_details,omitempty"`
	OutputDetails    *UsageDetails    `json:"output_details,omitempty"`
	ProviderDetails  map[string]int64 `json:"provider_details,omitempty"`
}

type UsageDetails struct {
	TextTokens      int64            `json:"text_tokens,omitempty"`
	AudioTokens     int64            `json:"audio_tokens,omitempty"`
	ReasoningTokens int64            `json:"reasoning_tokens,omitempty"`
	ToolTokens      int64            `json:"tool_tokens,omitempty"`
	CachedTokens    int64            `json:"cached_tokens,omitempty"`
	ByModality      map[string]int64 `json:"by_modality,omitempty"`
}

type Result struct {
	Provider      string
	Route         string
	Model         string
	AttemptID     string
	Headers       http.Header
	ContentType   string
	RawBody       []byte
	RawStream     io.ReadCloser
	Usage         Usage
	UsageSnapshot func() Usage
	StatusCode    int
}

// FinalUsage returns the latest usage extracted from a streaming response. If
// the stream did not report usage, it returns the provider's initial estimate.
func (r *Result) FinalUsage() Usage {
	if r == nil {
		return Usage{}
	}
	if r.UsageSnapshot != nil {
		if usage := r.UsageSnapshot(); !usage.IsZero() {
			return usage
		}
	}
	return r.Usage
}

func (u EstimatedUsage) TotalTokens() int64 {
	return saturatingUsageAdd(u.InputTokens, u.ReservedOutputTokens)
}

func (u ActualUsage) TotalTokens() int64 {
	return saturatingUsageAdd(u.InputTokens, u.OutputTokens)
}

func (u Usage) IsZero() bool {
	return u.InputTokens == 0 &&
		u.OutputTokens == 0 &&
		u.TotalTokens == 0 &&
		u.CacheReadTokens == 0 &&
		u.CacheWriteTokens == 0 &&
		u.InputDetails == nil &&
		u.OutputDetails == nil &&
		len(u.ProviderDetails) == 0
}

func (e *Request) Clone() *Request {
	if e == nil {
		return nil
	}
	clone := *e
	if e.Hints.Metadata != nil {
		clone.Hints.Metadata = cloneStringMap(e.Hints.Metadata)
	}
	if e.Hints.ProviderUnits != nil {
		clone.Hints.ProviderUnits = append([]string(nil), e.Hints.ProviderUnits...)
	}
	if len(e.RawBody) > 0 {
		clone.RawBody = append(json.RawMessage(nil), e.RawBody...)
	}
	if e.Meta.Headers != nil {
		clone.Meta.Headers = e.Meta.Headers.Clone()
	}
	return &clone
}

func (e *Request) Metadata() map[string]string {
	if e == nil {
		return nil
	}
	return e.Hints.Metadata
}

func (e *Request) RequestedMaxOutputTokens() int {
	if e == nil {
		return 0
	}
	return e.Hints.MaxOutputTokens
}

func (e *Request) UserValue() string {
	if e == nil {
		return ""
	}
	return e.Hints.User
}

func (e *Request) PromptText() string {
	if e == nil {
		return ""
	}
	return e.Hints.PromptText
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
