package providers

import (
	"bytes"
	"encoding/json"
	"slices"
	"strings"
)

func openAIProviderUnits(request map[string]json.RawMessage) []string {
	units := make(providerUnitSet)
	for _, tool := range requestTools(request) {
		switch kind := toolType(tool); {
		case strings.HasPrefix(kind, "web_search"):
			units.add("web_search_requests")
		case kind == "file_search":
			units.add("file_search_requests")
		case strings.HasPrefix(kind, "computer_use"):
			units.add("computer_use_requests")
		case kind == "code_interpreter":
			units.add("code_interpreter_requests")
		case kind == "image_generation":
			units.add("image_generation_requests")
		}
	}
	if presentJSON(request["web_search_options"]) {
		units.add("web_search_requests")
	}
	return units.sorted()
}

func anthropicProviderUnits(request map[string]json.RawMessage) []string {
	units := make(providerUnitSet)
	for _, tool := range requestTools(request) {
		if strings.HasPrefix(toolType(tool), "web_search") {
			units.add("web_search_requests")
		}
	}
	return units.sorted()
}

func geminiProviderUnits(request map[string]json.RawMessage) []string {
	units := make(providerUnitSet)
	for _, tool := range requestTools(request) {
		if presentJSON(tool["googleSearch"]) ||
			presentJSON(tool["google_search"]) ||
			presentJSON(tool["googleSearchRetrieval"]) ||
			presentJSON(tool["google_search_retrieval"]) {
			units.add("google_search_requests")
		}
	}
	return units.sorted()
}

func anthropicPromptCacheWrite(request map[string]json.RawMessage) bool {
	if hasEphemeralCacheControl(request) {
		return true
	}
	if contentPartsHaveEphemeralCacheControl(request["system"]) {
		return true
	}
	var messages []struct {
		Content json.RawMessage `json:"content"`
	}
	if json.Unmarshal(request["messages"], &messages) == nil {
		for _, message := range messages {
			if contentPartsHaveEphemeralCacheControl(message.Content) {
				return true
			}
		}
	}
	for _, tool := range requestTools(request) {
		if hasEphemeralCacheControl(tool) {
			return true
		}
	}
	return false
}

func contentPartsHaveEphemeralCacheControl(raw json.RawMessage) bool {
	var single map[string]json.RawMessage
	if json.Unmarshal(raw, &single) == nil && hasEphemeralCacheControl(single) {
		return true
	}
	var parts []map[string]json.RawMessage
	if json.Unmarshal(raw, &parts) != nil {
		return false
	}
	for _, part := range parts {
		if hasEphemeralCacheControl(part) {
			return true
		}
	}
	return false
}

func hasEphemeralCacheControl(object map[string]json.RawMessage) bool {
	var control map[string]json.RawMessage
	if json.Unmarshal(object["cache_control"], &control) != nil {
		return false
	}
	return strings.EqualFold(rawString(control["type"]), "ephemeral")
}

type providerUnitSet map[string]struct{}

func (units providerUnitSet) add(unit string) {
	if unit != "" {
		units[unit] = struct{}{}
	}
}

func (units providerUnitSet) sorted() []string {
	out := make([]string, 0, len(units))
	for unit := range units {
		out = append(out, unit)
	}
	slices.Sort(out)
	return out
}

func requestTools(request map[string]json.RawMessage) []map[string]json.RawMessage {
	var tools []map[string]json.RawMessage
	_ = json.Unmarshal(request["tools"], &tools)
	return tools
}

func toolType(tool map[string]json.RawMessage) string {
	return strings.ToLower(strings.TrimSpace(rawString(tool["type"])))
}

func rawString(raw json.RawMessage) string {
	var value string
	_ = json.Unmarshal(raw, &value)
	return value
}

func presentJSON(raw json.RawMessage) bool {
	raw = bytes.TrimSpace(raw)
	return len(raw) > 0 && !bytes.Equal(raw, []byte("null"))
}
