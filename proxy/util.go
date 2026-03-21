package proxy

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"unicode/utf8"

	"llmgw/gateway"
)

type RequestDecodeSpec struct {
	Provider        string
	Operation       gateway.Operation
	RequireModel    bool
	AllowQueryModel bool
	ModelPaths      [][]string
	StreamPaths     [][]string
	MaxOutputPaths  [][]string
	MetadataPaths   [][]string
	UserPaths       [][]string
}

func DecodeMinimalRequest(r *http.Request, maxBytes int64, spec RequestDecodeSpec) (*gateway.Request, error) {
	data, err := readRequestBody(r, maxBytes)
	if err != nil {
		return nil, err
	}
	raw := map[string]json.RawMessage{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, gateway.NewError(http.StatusBadRequest, "invalid_request_error", "invalid_json", err.Error())
	}
	modelPaths := spec.ModelPaths
	if len(modelPaths) == 0 {
		modelPaths = [][]string{{"model"}}
	}
	model := firstStringAtPaths(raw, modelPaths...)
	if model == "" && spec.AllowQueryModel {
		model = strings.TrimSpace(r.URL.Query().Get("model"))
	}
	if spec.RequireModel && model == "" {
		return nil, gateway.NewError(http.StatusBadRequest, "invalid_request_error", "missing_model", "model is required")
	}
	hints := gateway.RequestHints{
		Metadata:             firstStringMapAtPaths(raw, spec.MetadataPaths...),
		User:                 firstStringAtPaths(raw, spec.UserPaths...),
		MaxOutputTokens:      firstIntAtPaths(raw, spec.MaxOutputPaths...),
		EstimatedInputTokens: estimateTokensFromBody(data),
	}
	if hints.User == "" && hints.Metadata != nil {
		hints.User = hints.Metadata["user"]
	}
	return &gateway.Request{
		Provider:  spec.Provider,
		Operation: spec.Operation,
		Model:     model,
		Stream:    firstBoolAtPaths(raw, spec.StreamPaths...),
		RawBody:   append(json.RawMessage(nil), data...),
		Hints:     hints,
		Meta: gateway.Meta{
			Headers:    r.Header.Clone(),
			RemoteAddr: r.RemoteAddr,
			BodyBytes:  int64(len(data)),
		},
	}, nil
}

func readRequestBody(r *http.Request, maxBytes int64) ([]byte, error) {
	defer r.Body.Close()
	limited := io.LimitReader(r.Body, maxBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, gateway.NewError(http.StatusBadRequest, "invalid_request_error", "invalid_body", err.Error())
	}
	if int64(len(data)) > maxBytes {
		return nil, gateway.NewError(http.StatusRequestEntityTooLarge, "invalid_request_error", "body_too_large", "request body exceeds configured maximum")
	}
	return data, nil
}

func lookupRawPath(root map[string]json.RawMessage, path []string) json.RawMessage {
	if len(root) == 0 || len(path) == 0 {
		return nil
	}
	current := root
	for idx, key := range path {
		data := bytes.TrimSpace(current[key])
		if len(data) == 0 || bytes.Equal(data, []byte("null")) {
			return nil
		}
		if idx == len(path)-1 {
			return data
		}
		next := map[string]json.RawMessage{}
		if err := json.Unmarshal(data, &next); err != nil {
			return nil
		}
		current = next
	}
	return nil
}

func firstStringAtPaths(raw map[string]json.RawMessage, paths ...[]string) string {
	for _, path := range paths {
		data := lookupRawPath(raw, path)
		if len(data) == 0 {
			continue
		}
		var value string
		if err := json.Unmarshal(data, &value); err == nil {
			value = strings.TrimSpace(value)
			if value != "" {
				return value
			}
		}
	}
	return ""
}

func firstIntAtPaths(raw map[string]json.RawMessage, paths ...[]string) int {
	for _, path := range paths {
		data := lookupRawPath(raw, path)
		if len(data) == 0 {
			continue
		}
		var n int
		if err := json.Unmarshal(data, &n); err == nil {
			if n > 0 {
				return n
			}
			return 0
		}
		var text string
		if err := json.Unmarshal(data, &text); err == nil {
			n, err = strconv.Atoi(strings.TrimSpace(text))
			if err == nil {
				if n > 0 {
					return n
				}
				return 0
			}
		}
	}
	return 0
}

func firstBoolAtPaths(raw map[string]json.RawMessage, paths ...[]string) bool {
	for _, path := range paths {
		data := lookupRawPath(raw, path)
		if len(data) == 0 {
			continue
		}
		var value bool
		if err := json.Unmarshal(data, &value); err == nil {
			return value
		}
	}
	return false
}

func firstStringMapAtPaths(raw map[string]json.RawMessage, paths ...[]string) map[string]string {
	for _, path := range paths {
		data := lookupRawPath(raw, path)
		if len(data) == 0 {
			continue
		}
		var value map[string]string
		if err := json.Unmarshal(data, &value); err == nil && len(value) > 0 {
			return value
		}
	}
	return nil
}

func estimateTokensFromBody(data []byte) int64 {
	data = bytes.TrimSpace(data)
	if len(data) == 0 || bytes.Equal(data, []byte("null")) {
		return 0
	}
	tokens := int64(utf8.RuneCount(data) / 4)
	if tokens <= 0 {
		tokens = 1
	}
	return tokens
}
