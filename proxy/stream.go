package proxy

import (
	"bytes"
	"encoding/json"
	"io"
	"sync"

	"llmgw/gateway"
)

const maxUsageEventBytes = 1 << 20

// streamUsageTracker observes SSE data without changing the bytes returned to
// the caller. It intentionally keeps only one bounded event in memory.
type streamUsageTracker struct {
	codec    StreamCodec
	route    gateway.ResolvedRoute
	req      *gateway.Request
	terminal StreamTerminal

	mu       sync.Mutex
	usage    gateway.Usage
	pending  []byte
	event    []byte
	discard  bool
	finished bool
	complete bool
}

func newStreamUsageTracker(codec StreamCodec, route gateway.ResolvedRoute, req *gateway.Request) *streamUsageTracker {
	tracker := &streamUsageTracker{codec: codec, route: route, req: req}
	if codec.Terminal != nil {
		tracker.terminal = codec.Terminal(route, req)
	}
	return tracker
}

func (t *streamUsageTracker) Wrap(body io.ReadCloser) io.ReadCloser {
	return &usageTrackingReadCloser{body: body, tracker: t}
}

func (t *streamUsageTracker) Feed(data []byte) {
	if len(data) == 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.finished {
		return
	}
	t.pending = append(t.pending, data...)
	for {
		idx := bytes.IndexByte(t.pending, '\n')
		if idx < 0 {
			if len(t.pending) > maxUsageEventBytes {
				t.pending = t.pending[:0]
				t.event = t.event[:0]
				t.discard = true
			}
			return
		}
		line := t.pending[:idx]
		t.pending = t.pending[idx+1:]
		t.processLineLocked(line)
	}
}

func (t *streamUsageTracker) Finish() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.finished {
		return
	}
	if len(t.pending) > 0 {
		t.processLineLocked(t.pending)
		t.pending = nil
	}
	t.processEventLocked()
	t.finished = true
}

func (t *streamUsageTracker) Usage() gateway.Usage {
	t.mu.Lock()
	defer t.mu.Unlock()
	return cloneUsage(t.usage)
}

func (t *streamUsageTracker) Complete() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.complete
}

func (t *streamUsageTracker) processLineLocked(line []byte) {
	line = bytes.TrimSuffix(line, []byte{'\r'})
	if len(line) == 0 {
		t.processEventLocked()
		t.discard = false
		return
	}
	if t.discard || line[0] == ':' {
		return
	}
	if !bytes.HasPrefix(line, []byte("data:")) {
		return
	}
	data := line[len("data:"):]
	if len(data) > 0 && data[0] == ' ' {
		data = data[1:]
	}
	if len(t.event) > 0 {
		t.event = append(t.event, '\n')
	}
	if len(t.event)+len(data) > maxUsageEventBytes {
		t.event = t.event[:0]
		t.discard = true
		return
	}
	t.event = append(t.event, data...)
}

func (t *streamUsageTracker) processEventLocked() {
	if t.discard || len(t.event) == 0 {
		t.event = t.event[:0]
		return
	}
	data := bytes.TrimSpace(t.event)
	t.event = t.event[:0]
	if len(data) == 0 {
		return
	}
	if t.terminal != nil && t.terminal(data) {
		t.complete = true
	}
	if t.codec.Usage != nil {
		mergeUsage(&t.usage, t.codec.Usage(t.route, t.req, data))
	}
}

type usageTrackingReadCloser struct {
	body    io.ReadCloser
	tracker *streamUsageTracker
}

type responseLimitReadCloser struct {
	body      io.ReadCloser
	remaining int64
	overflow  error
}

func newResponseLimitReadCloser(body io.ReadCloser, limit int64) io.ReadCloser {
	return &responseLimitReadCloser{body: body, remaining: limit}
}

func (r *responseLimitReadCloser) Read(p []byte) (int, error) {
	if r.overflow != nil {
		return 0, r.overflow
	}
	if len(p) == 0 {
		return 0, nil
	}
	readSize := int64(len(p))
	if r.remaining < readSize {
		readSize = r.remaining + 1
	}
	n, err := r.body.Read(p[:int(readSize)])
	if int64(n) > r.remaining {
		allowed := int(r.remaining)
		r.remaining = 0
		r.overflow = responseTooLargeError()
		if allowed > 0 {
			return allowed, r.overflow
		}
		return 0, r.overflow
	}
	r.remaining -= int64(n)
	return n, err
}

func (r *responseLimitReadCloser) Close() error { return r.body.Close() }

func (r *usageTrackingReadCloser) Read(p []byte) (int, error) {
	n, err := r.body.Read(p)
	if n > 0 {
		r.tracker.Feed(p[:n])
	}
	if err != nil {
		r.tracker.Finish()
		if err == io.EOF && !r.tracker.Complete() {
			return n, gateway.NewError(502, "upstream_error", "truncated_stream", "upstream stream ended before a terminal event").
				WithDisposition(true, true, true)
		}
	}
	return n, err
}

func (r *usageTrackingReadCloser) Close() error {
	r.tracker.Finish()
	return r.body.Close()
}

func nestedStreamUsage(extract func([]byte) gateway.Usage, data []byte) gateway.Usage {
	if extract == nil {
		return gateway.Usage{}
	}
	usage := extract(data)
	var envelope struct {
		Response json.RawMessage `json:"response"`
		Message  json.RawMessage `json:"message"`
	}
	if json.Unmarshal(data, &envelope) != nil {
		return usage
	}
	if len(envelope.Response) > 0 {
		mergeUsage(&usage, extract(envelope.Response))
	}
	if len(envelope.Message) > 0 {
		mergeUsage(&usage, extract(envelope.Message))
	}
	return usage
}

func mergeUsage(dst *gateway.Usage, src gateway.Usage) {
	if dst == nil || src.IsZero() {
		return
	}
	dst.InputTokens = mergeUsageCounter(dst.InputTokens, src.InputTokens)
	dst.OutputTokens = mergeUsageCounter(dst.OutputTokens, src.OutputTokens)
	dst.TotalTokens = mergeUsageCounter(dst.TotalTokens, src.TotalTokens)
	dst.CacheReadTokens = mergeUsageCounter(dst.CacheReadTokens, src.CacheReadTokens)
	dst.CacheWriteTokens = mergeUsageCounter(dst.CacheWriteTokens, src.CacheWriteTokens)
	mergeUsageDetails(&dst.InputDetails, src.InputDetails)
	mergeUsageDetails(&dst.OutputDetails, src.OutputDetails)
	if len(src.ProviderDetails) > 0 {
		if dst.ProviderDetails == nil {
			dst.ProviderDetails = make(map[string]int64, len(src.ProviderDetails))
		}
		for key, value := range src.ProviderDetails {
			dst.ProviderDetails[key] = mergeUsageCounter(dst.ProviderDetails[key], value)
		}
	}
	if sum := saturatingProxyAdd(dst.InputTokens, dst.OutputTokens); dst.InputTokens >= 0 && dst.OutputTokens >= 0 && dst.TotalTokens >= 0 && dst.TotalTokens < sum {
		dst.TotalTokens = sum
	}
}

// Usage snapshots are cumulative, so retain their maximum. A negative value
// is malformed and remains visible for conservative fallback reconciliation.
func mergeUsageCounter(current, reported int64) int64 {
	if current < 0 || reported < 0 {
		return -1
	}
	return max(current, reported)
}

func mergeUsageDetails(dst **gateway.UsageDetails, src *gateway.UsageDetails) {
	if src == nil {
		return
	}
	if *dst == nil {
		*dst = &gateway.UsageDetails{}
	}
	(*dst).TextTokens = mergeUsageCounter((*dst).TextTokens, src.TextTokens)
	(*dst).AudioTokens = mergeUsageCounter((*dst).AudioTokens, src.AudioTokens)
	(*dst).ReasoningTokens = mergeUsageCounter((*dst).ReasoningTokens, src.ReasoningTokens)
	(*dst).ToolTokens = mergeUsageCounter((*dst).ToolTokens, src.ToolTokens)
	(*dst).CachedTokens = mergeUsageCounter((*dst).CachedTokens, src.CachedTokens)
	if len(src.ByModality) > 0 {
		if (*dst).ByModality == nil {
			(*dst).ByModality = make(map[string]int64, len(src.ByModality))
		}
		for key, value := range src.ByModality {
			(*dst).ByModality[key] = mergeUsageCounter((*dst).ByModality[key], value)
		}
	}
}

func cloneUsage(in gateway.Usage) gateway.Usage {
	out := in
	if in.InputDetails != nil {
		copy := *in.InputDetails
		copy.ByModality = cloneInt64Map(in.InputDetails.ByModality)
		out.InputDetails = &copy
	}
	if in.OutputDetails != nil {
		copy := *in.OutputDetails
		copy.ByModality = cloneInt64Map(in.OutputDetails.ByModality)
		out.OutputDetails = &copy
	}
	out.ProviderDetails = cloneInt64Map(in.ProviderDetails)
	return out
}

func cloneInt64Map(in map[string]int64) map[string]int64 {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]int64, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
