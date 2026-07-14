package proxy

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"strings"
	"unicode/utf8"

	"llmgw/gateway"
)

var multimodalKeyReplacer = strings.NewReplacer("_", "", "-", "")

type RequestDecodeSpec struct {
	Provider                string
	Operation               gateway.Operation
	RequireModel            bool
	AllowQueryModel         bool
	ModelPaths              [][]string
	StreamPaths             [][]string
	MaxOutputPaths          [][]string
	OutputMultiplicityPaths [][]string
	RequiredPaths           [][]string
	RequiredArrayPaths      [][]string
	RequiredObjectPaths     [][]string
	PositiveIntegerPaths    [][]string
	TextInputPaths          [][]string
	ResponsesInputPaths     [][]string
	MetadataPaths           [][]string
	UserPaths               [][]string
	DetectProviderUnits     func(map[string]json.RawMessage) []string
	DetectPromptCacheWrite  func(map[string]json.RawMessage) bool
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
	for _, path := range spec.RequiredPaths {
		data, present := lookupRawPathPresent(raw, path)
		if !present || !hasPresentJSON(data) {
			name := strings.Join(path, ".")
			apiErr := gateway.NewError(http.StatusBadRequest, "invalid_request_error", "missing_required_parameter", name+" is required")
			apiErr.Param = name
			return nil, apiErr
		}
	}
	for _, path := range spec.RequiredArrayPaths {
		if err := validateRequiredArray(raw, path); err != nil {
			return nil, err
		}
	}
	for _, path := range spec.RequiredObjectPaths {
		if err := validateRequiredObject(raw, path); err != nil {
			return nil, err
		}
	}
	for _, path := range spec.PositiveIntegerPaths {
		if err := validatePositiveInteger(raw, path); err != nil {
			return nil, err
		}
	}
	for _, path := range spec.MaxOutputPaths {
		if err := validateOptionalPositiveInteger(raw, path); err != nil {
			return nil, err
		}
	}
	for _, path := range spec.TextInputPaths {
		if err := validateTextInput(raw, path, false); err != nil {
			return nil, err
		}
	}
	for _, path := range spec.ResponsesInputPaths {
		if err := validateTextInput(raw, path, true); err != nil {
			return nil, err
		}
	}
	multiplicity, err := outputMultiplicityAtPaths(raw, spec.OutputMultiplicityPaths...)
	if err != nil {
		return nil, err
	}
	stream, err := optionalBoolAtPaths(raw, spec.StreamPaths...)
	if err != nil {
		return nil, err
	}
	hints := gateway.RequestHints{
		Metadata:             firstStringMapAtPaths(raw, spec.MetadataPaths...),
		User:                 firstStringAtPaths(raw, spec.UserPaths...),
		MaxOutputTokens:      maximumIntAtPaths(raw, spec.MaxOutputPaths...),
		OutputMultiplicity:   multiplicity,
		EstimatedInputTokens: estimateTokensFromBody(data),
	}
	features := detectRequiredFeatures(raw)
	hints.RequiresTools = features.tools
	hints.RequiresStructuredOutput = features.structured
	hints.RequiresVision = features.vision
	hints.RequiresAudio = features.audio
	hints.VisionInputParts = features.visionInputs
	hints.AudioInputParts = features.audioInputs
	hints.RequiresReasoning = features.reasoning
	if spec.DetectProviderUnits != nil {
		hints.ProviderUnits = spec.DetectProviderUnits(raw)
	}
	if len(hints.ProviderUnits) > 0 {
		hints.RequiresTools = true
	}
	if spec.DetectPromptCacheWrite != nil {
		hints.MayWritePromptCache = spec.DetectPromptCacheWrite(raw)
	}
	if hints.User == "" && hints.Metadata != nil {
		hints.User = hints.Metadata["user"]
	}
	return &gateway.Request{
		Provider:  spec.Provider,
		Operation: spec.Operation,
		Model:     model,
		Stream:    stream,
		RawBody:   append(json.RawMessage(nil), data...),
		Hints:     hints,
		Meta: gateway.Meta{
			Headers:    r.Header.Clone(),
			RemoteAddr: r.RemoteAddr,
			BodyBytes:  int64(len(data)),
		},
	}, nil
}

func requiredValue(raw map[string]json.RawMessage, path []string) (json.RawMessage, string, error) {
	name := strings.Join(path, ".")
	data, present := lookupRawPathPresent(raw, path)
	if !present || !hasPresentJSON(data) {
		apiErr := gateway.NewError(http.StatusBadRequest, "invalid_request_error", "missing_required_parameter", name+" is required")
		apiErr.Param = name
		return nil, name, apiErr
	}
	return data, name, nil
}

func invalidRequiredValue(name, expectation string) error {
	apiErr := gateway.NewError(http.StatusBadRequest, "invalid_request_error", "invalid_parameter", name+" "+expectation)
	apiErr.Param = name
	return apiErr
}

func validateRequiredArray(raw map[string]json.RawMessage, path []string) error {
	data, name, err := requiredValue(raw, path)
	if err != nil {
		return err
	}
	var values []json.RawMessage
	if json.Unmarshal(data, &values) != nil || len(values) == 0 {
		return invalidRequiredValue(name, "must be a non-empty array")
	}
	return nil
}

func validateRequiredObject(raw map[string]json.RawMessage, path []string) error {
	data, name, err := requiredValue(raw, path)
	if err != nil {
		return err
	}
	var value map[string]json.RawMessage
	if json.Unmarshal(data, &value) != nil {
		return invalidRequiredValue(name, "must be an object")
	}
	return nil
}

func validatePositiveInteger(raw map[string]json.RawMessage, path []string) error {
	data, name, err := requiredValue(raw, path)
	if err != nil {
		return err
	}
	var value int64
	if json.Unmarshal(data, &value) != nil || value <= 0 {
		return invalidRequiredValue(name, "must be a positive integer")
	}
	return nil
}

func validateOptionalPositiveInteger(raw map[string]json.RawMessage, path []string) error {
	data, present := lookupRawPathPresent(raw, path)
	if !present || !hasPresentJSON(data) {
		return nil
	}
	name := strings.Join(path, ".")
	var value int64
	if json.Unmarshal(data, &value) != nil || value <= 0 {
		return invalidRequiredValue(name, "must be a positive integer")
	}
	return nil
}

func validateTextInput(raw map[string]json.RawMessage, path []string, allowObject bool) error {
	data, name, err := requiredValue(raw, path)
	if err != nil {
		return err
	}
	var text string
	if json.Unmarshal(data, &text) == nil {
		if text == "" {
			return invalidRequiredValue(name, "must not be empty")
		}
		return nil
	}
	var values []json.RawMessage
	if json.Unmarshal(data, &values) == nil {
		if len(values) == 0 {
			return invalidRequiredValue(name, "must not be empty")
		}
		return nil
	}
	if allowObject {
		var object map[string]json.RawMessage
		if json.Unmarshal(data, &object) == nil {
			return nil
		}
	}
	return invalidRequiredValue(name, "has an invalid type")
}

func optionalBoolAtPaths(raw map[string]json.RawMessage, paths ...[]string) (bool, error) {
	for _, path := range paths {
		data, present := lookupRawPathPresent(raw, path)
		if !present {
			continue
		}
		var value bool
		if err := json.Unmarshal(data, &value); err != nil {
			name := strings.Join(path, ".")
			apiErr := gateway.NewError(http.StatusBadRequest, "invalid_request_error", "invalid_stream", name+" must be a boolean")
			apiErr.Param = name
			return false, apiErr
		}
		return value, nil
	}
	return false, nil
}

func outputMultiplicityAtPaths(raw map[string]json.RawMessage, paths ...[]string) (int64, error) {
	multiplier := int64(1)
	for _, path := range paths {
		data, present := lookupRawPathPresent(raw, path)
		if !present {
			continue
		}
		var value int64
		if err := json.Unmarshal(data, &value); err != nil || value <= 0 {
			name := strings.Join(path, ".")
			return 0, gateway.NewError(http.StatusBadRequest, "invalid_request_error", "invalid_output_multiplicity", name+" must be a positive integer")
		}
		if value > multiplier {
			multiplier = value
		}
	}
	return multiplier, nil
}

func lookupRawPathPresent(root map[string]json.RawMessage, path []string) (json.RawMessage, bool) {
	if len(root) == 0 || len(path) == 0 {
		return nil, false
	}
	current := root
	for idx, key := range path {
		data, ok := current[key]
		if !ok {
			return nil, false
		}
		if idx == len(path)-1 {
			return bytes.TrimSpace(data), true
		}
		next := map[string]json.RawMessage{}
		if err := json.Unmarshal(data, &next); err != nil {
			return nil, false
		}
		current = next
	}
	return nil, false
}

func readRequestBody(r *http.Request, maxBytes int64) ([]byte, error) {
	defer r.Body.Close()
	limited := io.LimitReader(r.Body, saturatingProxyAdd(maxBytes, 1))
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

func maximumIntAtPaths(raw map[string]json.RawMessage, paths ...[]string) int {
	maximum := 0
	for _, path := range paths {
		data := lookupRawPath(raw, path)
		if len(data) == 0 {
			continue
		}
		var n int64
		if err := json.Unmarshal(data, &n); err == nil {
			if n > int64(math.MaxInt) {
				return math.MaxInt
			}
			if n > int64(maximum) {
				maximum = int(n)
			}
		}
	}
	return maximum
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

type requiredFeatures struct {
	tools        bool
	structured   bool
	vision       bool
	audio        bool
	reasoning    bool
	visionInputs int64
	audioInputs  int64
}

func detectRequiredFeatures(raw map[string]json.RawMessage) requiredFeatures {
	features := requiredFeatures{
		tools: hasMeaningfulJSON(raw["tools"]) ||
			hasMeaningfulJSON(raw["functions"]) ||
			hasEnabledChoice(raw["tool_choice"]) ||
			hasEnabledChoice(raw["function_call"]),
		structured: requiresStructuredOutput(raw["response_format"]) ||
			requiresStructuredOutput(raw["output_format"]) ||
			requiresStructuredOutput(lookupRawPath(raw, []string{"text", "format"})) ||
			hasMeaningfulJSON(lookupRawPath(raw, []string{"generationConfig", "responseSchema"})) ||
			hasMeaningfulJSON(lookupRawPath(raw, []string{"generation_config", "response_schema"})) ||
			jsonStringContains(lookupRawPath(raw, []string{"generationConfig", "responseMimeType"}), "json") ||
			jsonStringContains(lookupRawPath(raw, []string{"generation_config", "response_mime_type"}), "json"),
		reasoning: requiresReasoning(raw["reasoning"]) ||
			requiresReasoning(raw["reasoning_effort"]) ||
			requiresReasoning(raw["thinking"]) ||
			requiresReasoning(lookupRawPath(raw, []string{"generationConfig", "thinkingConfig"})) ||
			requiresReasoning(lookupRawPath(raw, []string{"generation_config", "thinking_config"})),
	}
	features.audio = hasMeaningfulJSON(raw["audio"])
	var modalities []string
	if json.Unmarshal(raw["modalities"], &modalities) == nil {
		for _, modality := range modalities {
			features.audio = features.audio || strings.EqualFold(modality, "audio")
		}
	}
	// Only inspect content-bearing fields. Walking metadata or tool schemas can
	// misclassify schema vocabulary such as a property named "image" as input.
	for _, field := range []string{"messages", "input", "contents", "content"} {
		var decoded any
		if data := raw[field]; len(data) > 0 && json.Unmarshal(data, &decoded) == nil {
			detectMultimodal(decoded, &features, false, false)
		}
	}
	return features
}

func hasMeaningfulJSON(data json.RawMessage) bool {
	data = bytes.TrimSpace(data)
	if len(data) == 0 || bytes.Equal(data, []byte("null")) || bytes.Equal(data, []byte("false")) || bytes.Equal(data, []byte("[]")) || bytes.Equal(data, []byte("{}")) {
		return false
	}
	return true
}

func hasEnabledChoice(data json.RawMessage) bool {
	if !hasMeaningfulJSON(data) {
		return false
	}
	var value string
	if json.Unmarshal(data, &value) == nil {
		switch strings.ToLower(strings.TrimSpace(value)) {
		case "", "none", "disabled", "off", "false":
			return false
		}
	}
	return true
}

func jsonStringContains(data json.RawMessage, want string) bool {
	var value string
	return json.Unmarshal(data, &value) == nil && strings.Contains(strings.ToLower(value), strings.ToLower(want))
}

func requiresStructuredOutput(data json.RawMessage) bool {
	if !hasMeaningfulJSON(data) {
		return false
	}
	var text string
	if json.Unmarshal(data, &text) == nil {
		text = strings.ToLower(strings.TrimSpace(text))
		return text != "" && text != "text" && text != "none"
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(data, &object) != nil {
		return true
	}
	kind := strings.ToLower(firstStringAtPaths(object, []string{"type"}))
	if kind == "text" || kind == "none" {
		return false
	}
	return kind != "" || hasMeaningfulJSON(object["schema"]) || hasMeaningfulJSON(object["json_schema"])
}

func requiresReasoning(data json.RawMessage) bool {
	if !hasEnabledChoice(data) {
		return false
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(data, &object) != nil {
		return true
	}
	for _, key := range []string{"type", "effort"} {
		if value := firstStringAtPaths(object, []string{key}); value != "" {
			switch strings.ToLower(value) {
			case "none", "disabled", "off", "false":
				return false
			default:
				return true
			}
		}
	}
	for _, key := range []string{"thinkingBudget", "thinking_budget"} {
		if value := lookupRawPath(object, []string{key}); len(value) > 0 {
			var budget int
			return json.Unmarshal(value, &budget) != nil || budget != 0
		}
	}
	return hasMeaningfulJSON(data)
}

func detectMultimodal(value any, features *requiredFeatures, suppressVision, suppressAudio bool) {
	switch value := value.(type) {
	case []any:
		for _, item := range value {
			detectMultimodal(item, features, suppressVision, suppressAudio)
		}
	case map[string]any:
		var localVision, localAudio bool
		for key, child := range value {
			normalized := multimodalKeyReplacer.Replace(strings.ToLower(key))
			switch normalized {
			case "toolcalls", "toolcallid", "functioncall", "functioncalloutput", "functionresponse", "tooluse", "toolresult", "tooluseid":
				if child != nil {
					features.tools = true
				}
			case "imageurl", "inputimage":
				if child != nil {
					localVision = true
				}
			case "inputaudio", "audiourl":
				if child != nil {
					localAudio = true
				}
			case "type":
				if text, ok := child.(string); ok {
					kind := strings.ToLower(text)
					localVision = localVision || strings.Contains(kind, "image")
					localAudio = localAudio || strings.Contains(kind, "audio")
					features.tools = features.tools ||
						strings.Contains(kind, "function_call") || strings.Contains(kind, "function_response") ||
						strings.Contains(kind, "tool_call") || strings.Contains(kind, "tool_use") || strings.Contains(kind, "tool_result")
				}
			case "role":
				if text, ok := child.(string); ok && strings.EqualFold(text, "tool") {
					features.tools = true
				}
			case "mimetype", "mediatype":
				if text, ok := child.(string); ok {
					mime := strings.ToLower(text)
					localVision = localVision || strings.HasPrefix(mime, "image/")
					localAudio = localAudio || strings.HasPrefix(mime, "audio/")
				}
			case "modalities":
				if list, ok := child.([]any); ok {
					for _, item := range list {
						if text, ok := item.(string); ok && strings.EqualFold(text, "audio") {
							features.audio = true
						}
					}
				}
			}
		}
		features.vision = features.vision || localVision
		features.audio = features.audio || localAudio
		if localVision && !suppressVision {
			features.visionInputs++
		}
		if localAudio && !suppressAudio {
			features.audioInputs++
		}
		for _, child := range value {
			detectMultimodal(child, features, suppressVision || localVision, suppressAudio || localAudio)
		}
	}
}
