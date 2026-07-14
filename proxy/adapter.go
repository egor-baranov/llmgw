package proxy

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"llmgw/gateway"
)

// PreparedRequest is the provider-native HTTP request produced by an operation
// adapter. The generic proxy runtime owns transport, limits, and settlement;
// adapters own only provider protocol details.
type PreparedRequest struct {
	Body []byte
	URL  string
}

// OperationAdapter contains the protocol hooks that differ by operation for a
// provider. Nil validation deliberately permits opaque custom provider bodies.
type OperationAdapter struct {
	Prepare          func(gateway.ResolvedRoute, *gateway.Request) (PreparedRequest, error)
	ValidateResponse func([]byte) error
}

// StreamTerminal observes one decoded SSE data event. A fresh terminal is
// created per invocation so protocols with independently finishing candidates
// can retain bounded stream-local state without sharing it across requests.
type StreamTerminal func([]byte) bool

// StreamCodec observes provider SSE events and optionally transforms the raw
// stream. Transform reports whether it changed the representation so the core
// can discard stale integrity headers and enforce the output-side byte cap.
type StreamCodec struct {
	Usage     func(gateway.ResolvedRoute, *gateway.Request, []byte) gateway.Usage
	Terminal  func(gateway.ResolvedRoute, *gateway.Request) StreamTerminal
	Transform func(gateway.ResolvedRoute, *gateway.Request, io.ReadCloser) (io.ReadCloser, bool, error)
}

// Adapter is the compile-time provider contract used by Provider. Hooks are
// plain functions rather than a large method interface so adapters stay dense
// and operation support remains visible in one map.
type Adapter struct {
	Name              string
	Operations        map[gateway.Operation]OperationAdapter
	ApplyAuth         func(*http.Request, *gateway.Route) error
	ForwardHeaders    func(http.Header, http.Header)
	ParseError        func(int, []byte) error
	ExtractUsage      func(gateway.ResolvedRoute, *gateway.Request, []byte) gateway.Usage
	Stream            StreamCodec
	Preflight         func(gateway.ResolvedRoute, *gateway.Request) error
	TransformResponse func(gateway.ResolvedRoute, *gateway.Request, []byte) ([]byte, bool, error)
	ValidateRoute     func(*gateway.Route) error
	PlanBridge        func(*gateway.Route, *gateway.Request) (gateway.Operation, string, bool)
	ProjectTokenText  func(json.RawMessage) string
}

func (a Adapter) operation(op gateway.Operation) (OperationAdapter, bool) {
	operation, ok := a.Operations[op]
	return operation, ok && operation.Prepare != nil
}

func cloneAdapter(adapter Adapter) Adapter {
	if adapter.Operations == nil {
		return adapter
	}
	operations := make(map[gateway.Operation]OperationAdapter, len(adapter.Operations))
	for operation, hooks := range adapter.Operations {
		operations[operation] = hooks
	}
	adapter.Operations = operations
	return adapter
}

func decodeResponseObject(data []byte) (map[string]json.RawMessage, error) {
	var object map[string]json.RawMessage
	if json.Unmarshal(data, &object) != nil || object == nil {
		return nil, invalidUpstreamResponse("response is not a JSON object")
	}
	if raw := bytes.TrimSpace(object["error"]); len(raw) > 0 && !bytes.Equal(raw, []byte("null")) {
		return nil, invalidUpstreamResponse("successful response contains an error envelope")
	}
	return object, nil
}

func responseHasID(object map[string]json.RawMessage) bool {
	var id string
	return json.Unmarshal(object["id"], &id) == nil && strings.TrimSpace(id) != ""
}

func responseHasArray(object map[string]json.RawMessage, key string) bool {
	raw := bytes.TrimSpace(object[key])
	if len(raw) == 0 || raw[0] != '[' {
		return false
	}
	var values []json.RawMessage
	return json.Unmarshal(raw, &values) == nil
}
