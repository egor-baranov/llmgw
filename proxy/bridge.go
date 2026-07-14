package proxy

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"

	"llmgw/gateway"
)

type bridgeChatMessage struct {
	Role       string `json:"role"`
	Content    any    `json:"content"`
	ToolCallID string `json:"tool_call_id,omitempty"`
}

func bridgeOpenAIRequest(from gateway.Operation, body json.RawMessage) ([]byte, error) {
	switch from {
	case gateway.OpResponses:
		return bridgeResponsesRequest(body)
	case gateway.OpCompletions:
		return bridgeCompletionsRequest(body)
	default:
		return nil, gateway.UnsupportedOperation("unsupported OpenAI compatibility bridge")
	}
}

func bridgeResponsesRequest(body json.RawMessage) ([]byte, error) {
	source := map[string]json.RawMessage{}
	if err := json.Unmarshal(body, &source); err != nil {
		return nil, gateway.NewError(http.StatusBadRequest, "invalid_request_error", "invalid_json", err.Error())
	}
	if field := firstUnsupportedBridgeField(source, map[string]struct{}{
		"model": {}, "input": {}, "instructions": {}, "stream": {},
		"temperature": {}, "top_p": {}, "parallel_tool_calls": {}, "service_tier": {},
		"store": {}, "metadata": {}, "user": {}, "safety_identifier": {},
		"prompt_cache_key": {}, "seed": {}, "max_output_tokens": {}, "tools": {},
		"tool_choice": {}, "text": {}, "reasoning": {},
		"previous_response_id": {}, "conversation": {}, "background": {}, "include": {},
		"truncation": {}, "max_tool_calls": {}, "prompt": {},
	}); field != "" {
		return nil, gateway.UnsupportedOperation(fmt.Sprintf("responses field %q cannot be represented by chat completions", field))
	}
	if firstBoolAtPaths(source, []string{"stream"}) {
		return nil, gateway.UnsupportedOperation("responses-to-chat bridge does not support streaming")
	}
	if err := validateOptionalBridgeBool(source["stream"], "stream"); err != nil {
		return nil, err
	}
	if field := firstMeaningfulField(source,
		"previous_response_id", "conversation", "background", "include", "truncation", "max_tool_calls", "prompt"); field != "" {
		return nil, gateway.UnsupportedOperation(fmt.Sprintf("responses field %q cannot be represented by chat completions", field))
	}
	if field := firstUnsupportedNestedField(source["reasoning"], "effort"); field != "" {
		return nil, gateway.UnsupportedOperation(fmt.Sprintf("responses reasoning field %q cannot be represented by chat completions", field))
	}
	if field := firstUnsupportedNestedField(source["text"], "format"); field != "" {
		return nil, gateway.UnsupportedOperation(fmt.Sprintf("responses text field %q cannot be represented by chat completions", field))
	}
	messages, err := responseInputMessages(source["input"])
	if err != nil {
		return nil, err
	}
	if instructions := bytes.TrimSpace(source["instructions"]); len(instructions) > 0 && !bytes.Equal(instructions, []byte("null")) {
		var text string
		if err := json.Unmarshal(instructions, &text); err != nil {
			return nil, gateway.UnsupportedOperation("responses instructions must be a string for the chat bridge")
		}
		messages = append([]bridgeChatMessage{{Role: "system", Content: text}}, messages...)
	}
	if len(messages) == 0 {
		return nil, gateway.NewError(http.StatusBadRequest, "invalid_request_error", "missing_input", "input is required")
	}

	target := map[string]json.RawMessage{}
	if err := putJSON(target, "messages", messages); err != nil {
		return nil, err
	}
	copyRawFields(target, source,
		"temperature", "top_p", "parallel_tool_calls", "service_tier", "store", "metadata",
		"user", "safety_identifier", "prompt_cache_key", "seed")
	if value := source["max_output_tokens"]; hasMeaningfulJSON(value) {
		target["max_completion_tokens"] = value
	}
	if value := source["tools"]; hasMeaningfulJSON(value) {
		converted, err := bridgeResponseTools(value)
		if err != nil {
			return nil, err
		}
		if err := putJSON(target, "tools", converted); err != nil {
			return nil, err
		}
	}
	if value := source["tool_choice"]; hasMeaningfulJSON(value) {
		converted, err := bridgeResponseToolChoice(value)
		if err != nil {
			return nil, err
		}
		if err := putJSON(target, "tool_choice", converted); err != nil {
			return nil, err
		}
	}
	if format := lookupRawPath(source, []string{"text", "format"}); hasMeaningfulJSON(format) {
		converted, err := bridgeResponseFormat(format)
		if err != nil {
			return nil, err
		}
		if converted != nil {
			if err := putJSON(target, "response_format", converted); err != nil {
				return nil, err
			}
		}
	}
	if effort := lookupRawPath(source, []string{"reasoning", "effort"}); hasMeaningfulJSON(effort) {
		target["reasoning_effort"] = effort
	}
	return json.Marshal(target)
}

func bridgeCompletionsRequest(body json.RawMessage) ([]byte, error) {
	source := map[string]json.RawMessage{}
	if err := json.Unmarshal(body, &source); err != nil {
		return nil, gateway.NewError(http.StatusBadRequest, "invalid_request_error", "invalid_json", err.Error())
	}
	if field := firstUnsupportedBridgeField(source, map[string]struct{}{
		"model": {}, "prompt": {}, "stream": {}, "echo": {}, "suffix": {}, "logprobs": {},
		"best_of": {}, "temperature": {}, "top_p": {}, "n": {}, "stop": {},
		"presence_penalty": {}, "frequency_penalty": {}, "logit_bias": {}, "user": {},
		"seed": {}, "max_tokens": {},
	}); field != "" {
		return nil, gateway.UnsupportedOperation(fmt.Sprintf("completions field %q cannot be represented by chat completions", field))
	}
	if err := validateOptionalBridgeBool(source["stream"], "stream"); err != nil {
		return nil, err
	}
	var prompt string
	if err := json.Unmarshal(source["prompt"], &prompt); err != nil {
		return nil, gateway.UnsupportedOperation("completions-to-chat bridge requires a string prompt")
	}
	if err := validateOptionalBridgeBool(source["echo"], "echo"); err != nil {
		return nil, err
	}
	if hasPresentJSON(source["logprobs"]) || enabledJSONBool(source["echo"]) || hasMeaningfulJSON(source["suffix"]) || jsonIntGreaterThan(source["best_of"], 1) {
		return nil, gateway.UnsupportedOperation("requested legacy completion fields cannot be represented by chat completions")
	}
	if raw := source["best_of"]; hasPresentJSON(raw) {
		var value int64
		if json.Unmarshal(raw, &value) != nil {
			return nil, gateway.UnsupportedOperation("completions field \"best_of\" must be an integer for the chat bridge")
		}
	}
	target := map[string]json.RawMessage{}
	if err := putJSON(target, "messages", []bridgeChatMessage{{Role: "user", Content: prompt}}); err != nil {
		return nil, err
	}
	copyRawFields(target, source,
		"stream", "temperature", "top_p", "n", "stop", "presence_penalty", "frequency_penalty", "logit_bias", "user", "seed")
	if enabledJSONBool(source["stream"]) {
		if err := putJSON(target, "stream_options", map[string]bool{"include_usage": true}); err != nil {
			return nil, err
		}
	}
	if value := source["max_tokens"]; hasMeaningfulJSON(value) {
		target["max_completion_tokens"] = value
	}
	return json.Marshal(target)
}

// completionBridgeReadCloser converts OpenAI chat-completion SSE chunks into
// the legacy completions chunk shape without buffering the stream. The
// provider usage tracker wraps the source before this transformer, so usage
// accounting and terminal-event validation continue to observe the original
// provider protocol.
type completionBridgeReadCloser struct {
	body       io.ReadCloser
	reader     *bufio.Reader
	model      string
	pending    []byte
	pendingErr error
}

func newCompletionBridgeReadCloser(body io.ReadCloser, model string) io.ReadCloser {
	return &completionBridgeReadCloser{body: body, reader: bufio.NewReader(body), model: model}
}

func (r *completionBridgeReadCloser) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for len(r.pending) == 0 {
		if r.pendingErr != nil {
			err := r.pendingErr
			r.pendingErr = nil
			return 0, err
		}
		event, err := readSSEEvent(r.reader)
		if len(event) == 0 {
			return 0, err
		}
		// A bounded reader can return a partial event together with a precise
		// response-cap error. Never reinterpret those bytes as malformed JSON or
		// the cap error would incorrectly count as a provider circuit failure.
		if err != nil && !errors.Is(err, io.EOF) {
			return 0, err
		}
		converted, convertErr := bridgeChatCompletionSSEEvent(event, r.model)
		if convertErr != nil {
			return 0, convertErr
		}
		r.pending = converted
		r.pendingErr = err
	}
	n := copy(p, r.pending)
	r.pending = r.pending[n:]
	return n, nil
}

func (r *completionBridgeReadCloser) Close() error { return r.body.Close() }

func readSSEEvent(reader *bufio.Reader) ([]byte, error) {
	var event []byte
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			event = append(event, line...)
		}
		if err != nil {
			return event, err
		}
		if isSSEBlankLine(line) {
			return event, nil
		}
	}
}

func isSSEBlankLine(line []byte) bool {
	line = bytes.TrimSuffix(line, []byte("\n"))
	line = bytes.TrimSuffix(line, []byte("\r"))
	return len(line) == 0
}

func bridgeChatCompletionSSEEvent(event []byte, publicModel string) ([]byte, error) {
	lines := splitSSELines(event)
	data := make([][]byte, 0, len(lines))
	firstData := -1
	for index, line := range lines {
		content, _ := splitSSELineEnding(line)
		if payload, ok := sseDataPayload(content); ok {
			if firstData < 0 {
				firstData = index
			}
			data = append(data, payload)
		}
	}
	if firstData < 0 {
		return append([]byte(nil), event...), nil
	}
	payload := bytes.TrimSpace(bytes.Join(data, []byte("\n")))
	if len(payload) == 0 || bytes.Equal(payload, []byte("[DONE]")) {
		return append([]byte(nil), event...), nil
	}
	converted, err := bridgeChatCompletionPayload(payload, publicModel)
	if err != nil {
		return nil, err
	}
	var out []byte
	inserted := false
	for _, line := range lines {
		content, ending := splitSSELineEnding(line)
		if _, ok := sseDataPayload(content); ok {
			if inserted {
				continue
			}
			out = append(out, "data: "...)
			out = append(out, converted...)
			out = append(out, ending...)
			inserted = true
			continue
		}
		out = append(out, line...)
	}
	return out, nil
}

func splitSSELines(event []byte) [][]byte {
	lines := make([][]byte, 0, bytes.Count(event, []byte("\n"))+1)
	for len(event) > 0 {
		index := bytes.IndexByte(event, '\n')
		if index < 0 {
			lines = append(lines, event)
			break
		}
		lines = append(lines, event[:index+1])
		event = event[index+1:]
	}
	return lines
}

func splitSSELineEnding(line []byte) ([]byte, []byte) {
	switch {
	case bytes.HasSuffix(line, []byte("\r\n")):
		return line[:len(line)-2], line[len(line)-2:]
	case bytes.HasSuffix(line, []byte("\n")):
		return line[:len(line)-1], line[len(line)-1:]
	default:
		return line, nil
	}
}

func sseDataPayload(line []byte) ([]byte, bool) {
	if bytes.Equal(line, []byte("data")) {
		return nil, true
	}
	if !bytes.HasPrefix(line, []byte("data:")) {
		return nil, false
	}
	payload := line[len("data:"):]
	if len(payload) > 0 && payload[0] == ' ' {
		payload = payload[1:]
	}
	return payload, true
}

func bridgeChatCompletionPayload(payload []byte, publicModel string) ([]byte, error) {
	var chunk struct {
		ID                string          `json:"id"`
		Created           int64           `json:"created"`
		Model             string          `json:"model"`
		SystemFingerprint string          `json:"system_fingerprint"`
		ServiceTier       string          `json:"service_tier"`
		Obfuscation       string          `json:"obfuscation"`
		Usage             json.RawMessage `json:"usage"`
		Choices           []struct {
			Index int `json:"index"`
			Delta struct {
				Content      json.RawMessage   `json:"content"`
				Refusal      json.RawMessage   `json:"refusal"`
				ToolCalls    []json.RawMessage `json:"tool_calls"`
				FunctionCall json.RawMessage   `json:"function_call"`
				Audio        json.RawMessage   `json:"audio"`
			} `json:"delta"`
			FinishReason json.RawMessage `json:"finish_reason"`
			Logprobs     json.RawMessage `json:"logprobs"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(payload, &chunk); err != nil || chunk.ID == "" {
		return nil, invalidUpstreamResponse("chat bridge received an invalid stream chunk")
	}
	choices := make([]any, 0, len(chunk.Choices))
	for _, choice := range chunk.Choices {
		if rawJSONStringNonempty(choice.Delta.Refusal) || len(choice.Delta.ToolCalls) > 0 ||
			hasPresentJSON(choice.Delta.FunctionCall) || hasMeaningfulJSON(choice.Delta.Audio) || hasMeaningfulJSON(choice.Logprobs) {
			return nil, bridgeStreamUnsupported("chat stream output cannot be represented as a legacy completion")
		}
		text := ""
		if hasMeaningfulJSON(choice.Delta.Content) {
			if err := json.Unmarshal(choice.Delta.Content, &text); err != nil {
				return nil, bridgeStreamUnsupported("non-text chat stream output cannot be represented as a legacy completion")
			}
		}
		finishReason := any(nil)
		if hasPresentJSON(choice.FinishReason) {
			if err := json.Unmarshal(choice.FinishReason, &finishReason); err != nil {
				return nil, invalidUpstreamResponse("chat bridge received an invalid finish reason")
			}
		}
		choices = append(choices, map[string]any{
			"index": choice.Index, "text": text, "logprobs": nil, "finish_reason": finishReason,
		})
	}
	result := map[string]any{
		"id": chunk.ID, "object": "text_completion", "created": chunk.Created,
		"model": firstNonEmpty(publicModel, chunk.Model), "choices": choices,
	}
	if hasMeaningfulJSON(chunk.Usage) {
		var usage any
		if err := json.Unmarshal(chunk.Usage, &usage); err != nil {
			return nil, invalidUpstreamResponse("chat bridge received invalid usage")
		}
		result["usage"] = usage
	}
	if chunk.SystemFingerprint != "" {
		result["system_fingerprint"] = chunk.SystemFingerprint
	}
	if chunk.ServiceTier != "" {
		result["service_tier"] = chunk.ServiceTier
	}
	if chunk.Obfuscation != "" {
		result["obfuscation"] = chunk.Obfuscation
	}
	converted, err := json.Marshal(result)
	if err != nil {
		return nil, gateway.NewError(http.StatusInternalServerError, "server_error", "bridge_failed", "could not encode compatibility stream")
	}
	return converted, nil
}

func bridgeStreamUnsupported(message string) error {
	return gateway.UnsupportedOperation(message).WithDisposition(false, true, false)
}

func rawJSONStringNonempty(raw json.RawMessage) bool {
	if !hasPresentJSON(raw) {
		return false
	}
	var value string
	return json.Unmarshal(raw, &value) != nil || value != ""
}

func responseInputMessages(input json.RawMessage) ([]bridgeChatMessage, error) {
	if !hasMeaningfulJSON(input) {
		return nil, nil
	}
	var text string
	if json.Unmarshal(input, &text) == nil {
		return []bridgeChatMessage{{Role: "user", Content: text}}, nil
	}
	var items []any
	if err := decodeAny(input, &items); err != nil {
		return nil, gateway.UnsupportedOperation("responses input cannot be represented as chat messages")
	}
	messages := make([]bridgeChatMessage, 0, len(items))
	for _, item := range items {
		if text, ok := item.(string); ok {
			messages = append(messages, bridgeChatMessage{Role: "user", Content: text})
			continue
		}
		object, ok := item.(map[string]any)
		if !ok {
			return nil, gateway.UnsupportedOperation("responses input item cannot be represented as a chat message")
		}
		kind, _ := object["type"].(string)
		if kind == "function_call_output" {
			callID, _ := object["call_id"].(string)
			if callID == "" {
				return nil, gateway.UnsupportedOperation("function_call_output is missing call_id")
			}
			messages = append(messages, bridgeChatMessage{Role: "tool", ToolCallID: callID, Content: stringifyBridgeValue(object["output"])})
			continue
		}
		role, _ := object["role"].(string)
		if role == "" {
			return nil, gateway.UnsupportedOperation(fmt.Sprintf("responses input item type %q cannot be bridged", kind))
		}
		content, err := bridgeResponseContent(object["content"])
		if err != nil {
			return nil, err
		}
		messages = append(messages, bridgeChatMessage{Role: role, Content: content})
	}
	return messages, nil
}

func bridgeResponseContent(content any) (any, error) {
	if text, ok := content.(string); ok {
		return text, nil
	}
	parts, ok := content.([]any)
	if !ok {
		return nil, gateway.UnsupportedOperation("responses message content cannot be represented by chat completions")
	}
	out := make([]any, 0, len(parts))
	for _, part := range parts {
		object, ok := part.(map[string]any)
		if !ok {
			return nil, gateway.UnsupportedOperation("responses content part cannot be represented by chat completions")
		}
		kind, _ := object["type"].(string)
		switch kind {
		case "input_text", "output_text", "text":
			out = append(out, map[string]any{"type": "text", "text": object["text"]})
		case "input_image", "image_url":
			imageURL := object["image_url"]
			if imageURL == nil {
				return nil, gateway.UnsupportedOperation("file-based image input cannot be bridged to chat completions")
			}
			var bridgedImageURL any = imageURL
			switch value := imageURL.(type) {
			case string:
				bridgedImageURL = map[string]any{"url": value}
			case map[string]any:
				clone := make(map[string]any, len(value)+1)
				for key, field := range value {
					clone[key] = field
				}
				bridgedImageURL = clone
			}
			if detail, ok := object["detail"]; ok {
				target, ok := bridgedImageURL.(map[string]any)
				if !ok {
					return nil, gateway.UnsupportedOperation("responses image detail cannot be represented by chat completions")
				}
				target["detail"] = detail
			}
			out = append(out, map[string]any{"type": "image_url", "image_url": bridgedImageURL})
		case "input_audio":
			if object["input_audio"] == nil {
				return nil, gateway.UnsupportedOperation("responses audio input cannot be bridged to chat completions")
			}
			out = append(out, map[string]any{"type": "input_audio", "input_audio": object["input_audio"]})
		default:
			return nil, gateway.UnsupportedOperation(fmt.Sprintf("responses content part type %q cannot be bridged", kind))
		}
	}
	return out, nil
}

func bridgeResponseTools(raw json.RawMessage) ([]any, error) {
	var tools []map[string]any
	if err := decodeAny(raw, &tools); err != nil {
		return nil, gateway.NewError(http.StatusBadRequest, "invalid_request_error", "invalid_tools", "tools must be an array")
	}
	out := make([]any, 0, len(tools))
	for _, tool := range tools {
		kind, _ := tool["type"].(string)
		if kind != "function" {
			return nil, gateway.UnsupportedOperation(fmt.Sprintf("responses tool type %q cannot be bridged to chat completions", kind))
		}
		function := map[string]any{}
		for _, key := range []string{"name", "description", "parameters", "strict"} {
			if value, ok := tool[key]; ok {
				function[key] = value
			}
		}
		out = append(out, map[string]any{"type": "function", "function": function})
	}
	return out, nil
}

func bridgeResponseToolChoice(raw json.RawMessage) (any, error) {
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return text, nil
	}
	var choice map[string]any
	if err := decodeAny(raw, &choice); err != nil {
		return nil, gateway.NewError(http.StatusBadRequest, "invalid_request_error", "invalid_tool_choice", "invalid tool_choice")
	}
	if choice["type"] == "function" {
		name, _ := choice["name"].(string)
		if name == "" {
			return nil, gateway.UnsupportedOperation("function tool_choice is missing name")
		}
		return map[string]any{"type": "function", "function": map[string]any{"name": name}}, nil
	}
	return nil, gateway.UnsupportedOperation("responses tool_choice cannot be bridged to chat completions")
}

func bridgeResponseFormat(raw json.RawMessage) (any, error) {
	var format map[string]any
	if err := decodeAny(raw, &format); err != nil {
		return nil, gateway.NewError(http.StatusBadRequest, "invalid_request_error", "invalid_response_format", "invalid response format")
	}
	kind, _ := format["type"].(string)
	switch kind {
	case "", "text":
		return nil, nil
	case "json_object":
		return map[string]any{"type": "json_object"}, nil
	case "json_schema":
		delete(format, "type")
		return map[string]any{"type": "json_schema", "json_schema": format}, nil
	default:
		return nil, gateway.UnsupportedOperation(fmt.Sprintf("response format %q cannot be bridged to chat completions", kind))
	}
}

func bridgeOpenAIResponse(from gateway.Operation, publicModel string, body []byte) ([]byte, error) {
	switch from {
	case gateway.OpResponses:
		return bridgeChatToResponses(publicModel, body)
	case gateway.OpCompletions:
		return bridgeChatToCompletions(publicModel, body)
	default:
		return nil, gateway.UnsupportedOperation("unsupported OpenAI response compatibility bridge")
	}
}

type bridgeChatResponse struct {
	ID                string          `json:"id"`
	Created           int64           `json:"created"`
	Model             string          `json:"model"`
	SystemFingerprint string          `json:"system_fingerprint"`
	ServiceTier       string          `json:"service_tier"`
	Usage             json.RawMessage `json:"usage"`
	Choices           []struct {
		Index   int `json:"index"`
		Message struct {
			Role         string          `json:"role"`
			Content      json.RawMessage `json:"content"`
			Refusal      string          `json:"refusal"`
			FunctionCall json.RawMessage `json:"function_call"`
			ToolCalls    []struct {
				ID       string `json:"id"`
				Type     string `json:"type"`
				Function struct {
					Name      string `json:"name"`
					Arguments string `json:"arguments"`
				} `json:"function"`
			} `json:"tool_calls"`
		} `json:"message"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
}

func decodeBridgeChatResponse(body []byte) (bridgeChatResponse, error) {
	var chat bridgeChatResponse
	if err := json.Unmarshal(body, &chat); err != nil || chat.ID == "" || len(chat.Choices) == 0 {
		return bridgeChatResponse{}, gateway.NewError(http.StatusBadGateway, "upstream_error", "invalid_upstream_response", "chat bridge received an invalid upstream response").WithDisposition(false, true, true)
	}
	return chat, nil
}

func bridgeChatToResponses(publicModel string, body []byte) ([]byte, error) {
	chat, err := decodeBridgeChatResponse(body)
	if err != nil {
		return nil, err
	}
	output := make([]any, 0, len(chat.Choices))
	outputText := strings.Builder{}
	status := "completed"
	for _, choice := range chat.Choices {
		if hasPresentJSON(choice.Message.FunctionCall) {
			return nil, gateway.UnsupportedOperation("legacy chat function calls cannot be represented by responses")
		}
		text, err := chatContentText(choice.Message.Content)
		if err != nil {
			return nil, err
		}
		content := make([]any, 0, 2)
		if text != "" {
			content = append(content, map[string]any{"type": "output_text", "text": text, "annotations": []any{}})
			outputText.WriteString(text)
		}
		if choice.Message.Refusal != "" {
			content = append(content, map[string]any{"type": "refusal", "refusal": choice.Message.Refusal})
		}
		if len(content) > 0 {
			output = append(output, map[string]any{
				"id": "msg_" + bridgeIDSuffix(chat.ID, choice.Index), "type": "message", "status": "completed",
				"role": firstNonEmpty(choice.Message.Role, "assistant"), "content": content,
			})
		}
		for callIndex, call := range choice.Message.ToolCalls {
			output = append(output, map[string]any{
				"id": fmt.Sprintf("fc_%s_%d", bridgeIDSuffix(chat.ID, choice.Index), callIndex), "type": "function_call", "status": "completed",
				"call_id": call.ID, "name": call.Function.Name, "arguments": call.Function.Arguments,
			})
		}
		if choice.FinishReason == "length" {
			status = "incomplete"
		}
	}
	result := map[string]any{
		"id": "resp_" + strings.TrimPrefix(chat.ID, "chatcmpl-"), "object": "response", "created_at": chat.Created,
		"status": status, "model": firstNonEmpty(publicModel, chat.Model), "output": output, "output_text": outputText.String(),
	}
	if chat.SystemFingerprint != "" {
		result["system_fingerprint"] = chat.SystemFingerprint
	}
	if chat.ServiceTier != "" {
		result["service_tier"] = chat.ServiceTier
	}
	if len(chat.Usage) > 0 && !bytes.Equal(bytes.TrimSpace(chat.Usage), []byte("null")) {
		usage := extractOpenAIUsage(gateway.ResolvedRoute{}, nil, body)
		reasoningTokens := int64(0)
		if usage.OutputDetails != nil {
			reasoningTokens = usage.OutputDetails.ReasoningTokens
		}
		result["usage"] = map[string]any{
			"input_tokens":          usage.InputTokens,
			"input_tokens_details":  map[string]any{"cached_tokens": usage.CacheReadTokens},
			"output_tokens":         usage.OutputTokens,
			"output_tokens_details": map[string]any{"reasoning_tokens": reasoningTokens},
			"total_tokens":          usage.TotalTokens,
		}
	}
	if status == "incomplete" {
		result["incomplete_details"] = map[string]any{"reason": "max_output_tokens"}
	}
	return json.Marshal(result)
}

func bridgeChatToCompletions(publicModel string, body []byte) ([]byte, error) {
	chat, err := decodeBridgeChatResponse(body)
	if err != nil {
		return nil, err
	}
	choices := make([]any, 0, len(chat.Choices))
	for _, choice := range chat.Choices {
		if hasPresentJSON(choice.Message.FunctionCall) {
			return nil, gateway.UnsupportedOperation("legacy chat function calls cannot be represented as legacy completions")
		}
		if len(choice.Message.ToolCalls) > 0 {
			return nil, gateway.UnsupportedOperation("chat tool calls cannot be represented as legacy completions")
		}
		if choice.Message.Refusal != "" {
			return nil, gateway.UnsupportedOperation("chat refusals cannot be represented as legacy completions")
		}
		text, err := chatContentText(choice.Message.Content)
		if err != nil {
			return nil, err
		}
		choices = append(choices, map[string]any{
			"index": choice.Index, "text": text, "logprobs": nil, "finish_reason": choice.FinishReason,
		})
	}
	result := map[string]any{
		"id": chat.ID, "object": "text_completion", "created": chat.Created,
		"model": firstNonEmpty(publicModel, chat.Model), "choices": choices,
	}
	if chat.SystemFingerprint != "" {
		result["system_fingerprint"] = chat.SystemFingerprint
	}
	if chat.ServiceTier != "" {
		result["service_tier"] = chat.ServiceTier
	}
	if len(chat.Usage) > 0 {
		result["usage"] = chat.Usage
	}
	return json.Marshal(result)
}

func chatContentText(raw json.RawMessage) (string, error) {
	if len(bytes.TrimSpace(raw)) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return "", nil
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return text, nil
	}
	var parts []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if json.Unmarshal(raw, &parts) != nil {
		return "", gateway.UnsupportedOperation("chat response content cannot be represented by the compatibility bridge")
	}
	var out strings.Builder
	for _, part := range parts {
		if part.Type != "text" && part.Type != "output_text" {
			return "", gateway.UnsupportedOperation(fmt.Sprintf("chat response content part type %q cannot be represented by the compatibility bridge", part.Type))
		}
		out.WriteString(part.Text)
	}
	return out.String(), nil
}

func bridgeIDSuffix(id string, index int) string {
	return fmt.Sprintf("%s_%d", strings.TrimPrefix(id, "chatcmpl-"), index)
}

func copyRawFields(dst, src map[string]json.RawMessage, fields ...string) {
	for _, field := range fields {
		if value := src[field]; hasPresentJSON(value) {
			dst[field] = value
		}
	}
}

func firstMeaningfulField(source map[string]json.RawMessage, fields ...string) string {
	for _, field := range fields {
		if hasMeaningfulJSON(source[field]) {
			return field
		}
	}
	return ""
}

func firstUnsupportedBridgeField(source map[string]json.RawMessage, allowed map[string]struct{}) string {
	fields := make([]string, 0)
	for field, value := range source {
		if _, ok := allowed[field]; !ok && hasPresentJSON(value) {
			fields = append(fields, field)
		}
	}
	if len(fields) == 0 {
		return ""
	}
	sort.Strings(fields)
	return fields[0]
}

func hasPresentJSON(raw json.RawMessage) bool {
	raw = bytes.TrimSpace(raw)
	return len(raw) > 0 && !bytes.Equal(raw, []byte("null"))
}

func validateOptionalBridgeBool(raw json.RawMessage, field string) error {
	if !hasPresentJSON(raw) {
		return nil
	}
	var value bool
	if err := json.Unmarshal(raw, &value); err != nil {
		return gateway.UnsupportedOperation(fmt.Sprintf("field %q must be a boolean for the compatibility bridge", field))
	}
	return nil
}

func firstUnsupportedNestedField(raw json.RawMessage, allowed ...string) string {
	if !hasMeaningfulJSON(raw) {
		return ""
	}
	object := map[string]json.RawMessage{}
	if err := json.Unmarshal(raw, &object); err != nil {
		return "value"
	}
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, field := range allowed {
		allowedSet[field] = struct{}{}
	}
	return firstUnsupportedBridgeField(object, allowedSet)
}

func putJSON(dst map[string]json.RawMessage, key string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return gateway.NewError(http.StatusBadRequest, "invalid_request_error", "invalid_bridge_value", "request cannot be converted for the selected route")
	}
	dst[key] = data
	return nil
}

func decodeAny(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	return decoder.Decode(target)
}

func stringifyBridgeValue(value any) string {
	if text, ok := value.(string); ok {
		return text
	}
	data, _ := json.Marshal(value)
	return string(data)
}

func enabledJSONBool(raw json.RawMessage) bool {
	var value bool
	return json.Unmarshal(raw, &value) == nil && value
}

func jsonIntGreaterThan(raw json.RawMessage, threshold int64) bool {
	var value int64
	return json.Unmarshal(raw, &value) == nil && value > threshold
}
