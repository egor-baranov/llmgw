package proxy

import (
	"encoding/json"
	"strings"
)

func projectOpenAITokenText(raw json.RawMessage) string {
	return projectTokenText(raw, scrubOpenAIInlineMedia)
}

func projectAnthropicTokenText(raw json.RawMessage) string {
	return projectTokenText(raw, scrubAnthropicInlineMedia)
}

func projectGeminiTokenText(raw json.RawMessage) string {
	return projectTokenText(raw, scrubGeminiInlineMedia)
}

func projectTokenText(raw json.RawMessage, scrub func(map[string]any)) string {
	if len(raw) == 0 {
		return ""
	}
	var value any
	if json.Unmarshal(raw, &value) != nil {
		return string(raw)
	}
	if root, ok := value.(map[string]any); ok && scrub != nil {
		scrub(root)
	}
	projected, err := json.Marshal(value)
	if err != nil {
		return string(raw)
	}
	return string(projected)
}

func scrubOpenAIInlineMedia(root map[string]any) {
	forEachObject(root["messages"], func(message map[string]any) {
		forEachObject(message["content"], scrubOpenAIContentPart)
	})
	forEachObject(root["input"], func(item map[string]any) {
		if _, hasContent := item["content"]; hasContent {
			forEachObject(item["content"], scrubOpenAIContentPart)
			return
		}
		scrubOpenAIContentPart(item)
	})
}

func scrubOpenAIContentPart(part map[string]any) {
	switch lowerString(part["type"]) {
	case "image_url":
		switch image := part["image_url"].(type) {
		case map[string]any:
			if text, ok := image["url"].(string); ok && isBase64DataURI(text, "image/") {
				image["url"] = "[binary image]"
			}
		case string:
			if isBase64DataURI(image, "image/") {
				part["image_url"] = "[binary image]"
			}
		}
	case "input_image":
		if image, ok := part["image_url"].(string); ok && isBase64DataURI(image, "image/") {
			part["image_url"] = "[binary image]"
		}
	case "input_audio":
		if audio, ok := part["input_audio"].(map[string]any); ok {
			if data, ok := audio["data"].(string); ok && looksLikeBase64(strings.TrimSpace(data)) {
				audio["data"] = "[binary audio]"
			}
		}
	}
}

func scrubAnthropicInlineMedia(root map[string]any) {
	forEachObject(root["messages"], func(message map[string]any) {
		forEachObject(message["content"], scrubAnthropicContentPart)
	})
	forEachObject(root["system"], scrubAnthropicContentPart)
}

func scrubAnthropicContentPart(part map[string]any) {
	if lowerString(part["type"]) == "tool_result" {
		forEachObject(part["content"], scrubAnthropicContentPart)
		return
	}
	if lowerString(part["type"]) != "image" {
		return
	}
	source, ok := part["source"].(map[string]any)
	if !ok || lowerString(source["type"]) != "base64" || !strings.HasPrefix(lowerString(source["media_type"]), "image/") {
		return
	}
	if data, ok := source["data"].(string); ok && looksLikeBase64(strings.TrimSpace(data)) {
		source["data"] = "[binary image]"
	}
}

func scrubGeminiInlineMedia(root map[string]any) {
	scrubContent := func(content map[string]any) {
		forEachObject(content["parts"], func(part map[string]any) {
			for _, key := range []string{"inlineData", "inline_data"} {
				inline, ok := part[key].(map[string]any)
				if !ok {
					continue
				}
				mimeType := lowerString(inline["mimeType"])
				if mimeType == "" {
					mimeType = lowerString(inline["mime_type"])
				}
				if !strings.HasPrefix(mimeType, "image/") && !strings.HasPrefix(mimeType, "audio/") && !strings.HasPrefix(mimeType, "video/") {
					continue
				}
				if data, ok := inline["data"].(string); ok && looksLikeBase64(strings.TrimSpace(data)) {
					inline["data"] = "[binary media]"
				}
			}
		})
	}
	forEachObject(root["contents"], scrubContent)
	if system, ok := root["systemInstruction"].(map[string]any); ok {
		scrubContent(system)
	}
	if system, ok := root["system_instruction"].(map[string]any); ok {
		scrubContent(system)
	}
}

func forEachObject(value any, visit func(map[string]any)) {
	items, ok := value.([]any)
	if !ok {
		return
	}
	for _, item := range items {
		if object, ok := item.(map[string]any); ok {
			visit(object)
		}
	}
}

func lowerString(value any) string {
	text, _ := value.(string)
	return strings.ToLower(strings.TrimSpace(text))
}

func isBase64DataURI(value, mediaPrefix string) bool {
	lower := strings.ToLower(strings.TrimSpace(value))
	return strings.HasPrefix(lower, "data:"+mediaPrefix) && strings.Contains(lower, ";base64,")
}

func looksLikeBase64(value string) bool {
	if len(value) < 4 || len(value)%4 != 0 {
		return false
	}
	for _, r := range value {
		if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '+' || r == '/' || r == '=' || r == '\n' || r == '\r' {
			continue
		}
		return false
	}
	return true
}
