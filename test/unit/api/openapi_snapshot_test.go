package api_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	gwapi "llmgw/api"
	"llmgw/gateway"
)

func TestCheckedOpenAPISnapshotIsCurrent(t *testing.T) {
	t.Setenv("LLMGW_BEARER_TOKEN", "snapshot-test-token")
	t.Setenv("OPENAI_API_KEY", "snapshot-openai-key")
	t.Setenv("ANTHROPIC_API_KEY", "snapshot-anthropic-key")
	t.Setenv("GEMINI_API_KEY", "snapshot-gemini-key")
	root := filepath.Join("..", "..", "..")
	cfg, err := gateway.LoadConfigFile(filepath.Join(root, "config", "config.example.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	generated, err := gwapi.OpenAPIYAML(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range [][]byte{
		[]byte("anthropicGatewayKey:"),
		[]byte("geminiGatewayKey:"),
		[]byte("name: x-api-key"),
		[]byte("name: x-goog-api-key"),
	} {
		if !bytes.Contains(generated, want) {
			t.Fatalf("generated OpenAPI document is missing %q", want)
		}
	}
	checked, err := os.ReadFile(filepath.Join(root, "openapi.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(generated, checked) {
		t.Fatal("openapi.yaml is stale; regenerate it with go run ./cmd/llmgw -config config/config.example.yaml -print-openapi")
	}
}

func TestOpenAPIProviderNativeErrorContracts(t *testing.T) {
	t.Setenv("LLMGW_BEARER_TOKEN", "snapshot-test-token")
	t.Setenv("OPENAI_API_KEY", "snapshot-openai-key")
	t.Setenv("ANTHROPIC_API_KEY", "snapshot-anthropic-key")
	t.Setenv("GEMINI_API_KEY", "snapshot-gemini-key")
	root := filepath.Join("..", "..", "..")
	cfg, err := gateway.LoadConfigFile(filepath.Join(root, "config", "config.example.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	generated, err := gwapi.OpenAPIJSON(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(generated, &document); err != nil {
		t.Fatal(err)
	}

	openAI := defaultResponseExample(t, document, "/v1/chat/completions")
	openAIError := objectField(t, openAI, "error")
	if _, ok := openAIError["type"]; !ok {
		t.Fatalf("OpenAI error example = %#v, want type", openAI)
	}
	if _, ok := openAIError["status"]; ok {
		t.Fatalf("OpenAI error example = %#v, must not use Gemini status", openAI)
	}

	anthropic := defaultResponseExample(t, document, "/v1/messages")
	if anthropic["type"] != "error" || objectField(t, anthropic, "error")["type"] != "invalid_request_error" {
		t.Fatalf("Anthropic error example = %#v, want native envelope", anthropic)
	}

	gemini := defaultResponseExample(t, document, "/v1beta/models/{model}:generateContent")
	geminiError := objectField(t, gemini, "error")
	if geminiError["status"] != "INVALID_ARGUMENT" || geminiError["code"] != float64(400) {
		t.Fatalf("Gemini error example = %#v, want Google API envelope", gemini)
	}
}

func TestOpenAPIResponseSchemasMatchWireContracts(t *testing.T) {
	t.Setenv("LLMGW_BEARER_TOKEN", "snapshot-test-token")
	t.Setenv("OPENAI_API_KEY", "snapshot-openai-key")
	t.Setenv("ANTHROPIC_API_KEY", "snapshot-anthropic-key")
	t.Setenv("GEMINI_API_KEY", "snapshot-gemini-key")
	root := filepath.Join("..", "..", "..")
	cfg, err := gateway.LoadConfigFile(filepath.Join(root, "config", "config.example.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	document := openAPIJSONDocument(t, cfg)
	paths := objectField(t, document, "paths")
	openAPIGet := objectField(t, objectField(t, paths, "/openapi.json"), "get")
	openAPIResponses := objectField(t, openAPIGet, "responses")
	openAPIContent := objectField(t, objectField(t, openAPIResponses, "200"), "content")
	openAPISchema := objectField(t, objectField(t, openAPIContent, "application/json"), "schema")
	if openAPISchema["type"] != "object" || openAPISchema["additionalProperties"] != true {
		t.Fatalf("/openapi.json response schema = %#v, want arbitrary JSON object", openAPISchema)
	}

	responses := responseSchema(t, document, "/v1/responses", "200", "application/json")
	properties := objectField(t, responses, "properties")
	if _, ok := properties["created_at"]; !ok {
		t.Fatal("Responses schema is missing created_at")
	}
	if _, ok := properties["created"]; ok {
		t.Fatal("Responses schema incorrectly documents chat-style created")
	}
	usage := schemaProperty(t, responses, "usage")
	usageProperties := objectField(t, usage, "properties")
	for _, key := range []string{"input_tokens", "output_tokens", "input_tokens_details", "output_tokens_details"} {
		if _, ok := usageProperties[key]; !ok {
			t.Fatalf("Responses usage schema is missing %q", key)
		}
	}
	incomplete := schemaProperty(t, responses, "incomplete_details")
	if _, ok := objectField(t, incomplete, "properties")["reason"]; !ok {
		t.Fatal("Responses incomplete_details schema is missing reason")
	}
	outputItems := objectField(t, schemaProperty(t, responses, "output"), "items")
	outputProperties := objectField(t, outputItems, "properties")
	for _, key := range []string{"status", "call_id", "name", "arguments"} {
		if _, ok := outputProperties[key]; !ok {
			t.Fatalf("Responses output schema is missing %q", key)
		}
	}
	contentItems := objectField(t, schemaProperty(t, outputItems, "content"), "items")
	contentProperties := objectField(t, contentItems, "properties")
	if _, ok := contentProperties["annotations"]; !ok {
		t.Fatal("Responses content schema is missing annotations")
	}
	imageURL := objectField(t, contentProperties, "image_url")
	if oneOf, ok := imageURL["oneOf"].([]any); !ok || len(oneOf) != 2 {
		t.Fatalf("image_url schema = %#v, want string-or-object union", imageURL)
	}

	completions := responseSchema(t, document, "/v1/completions", "200", "application/json")
	choiceItems := objectField(t, schemaProperty(t, completions, "choices"), "items")
	logprobs, ok := objectField(t, choiceItems, "properties")["logprobs"].(map[string]any)
	if !ok {
		t.Fatal("Completion choice schema is missing logprobs")
	}
	if logprobs["nullable"] != true {
		t.Fatalf("Completion logprobs schema = %#v, want nullable", logprobs)
	}
	completionUsage := objectField(t, schemaProperty(t, completions, "usage"), "properties")
	for _, key := range []string{"prompt_tokens_details", "completion_tokens_details"} {
		if _, ok := completionUsage[key]; !ok {
			t.Fatalf("Completion usage schema is missing %q", key)
		}
	}
	messagesPost := objectField(t, objectField(t, paths, "/v1/messages"), "post")
	messagesResponses := objectField(t, messagesPost, "responses")
	messagesContent := objectField(t, objectField(t, messagesResponses, "200"), "content")
	if _, ok := messagesContent["text/event-stream"]; !ok {
		t.Fatal("Anthropic messages response is missing text/event-stream")
	}

	chat := responseSchema(t, document, "/v1/chat/completions", "200", "application/json")
	chatChoice := objectField(t, schemaProperty(t, chat, "choices"), "items")
	chatLogprobs := objectField(t, objectField(t, chatChoice, "properties"), "logprobs")
	if chatLogprobs["nullable"] != true {
		t.Fatalf("Chat logprobs schema = %#v, want nullable", chatLogprobs)
	}
	message := schemaProperty(t, chatChoice, "message")
	refusal := objectField(t, objectField(t, message, "properties"), "refusal")
	if refusal["nullable"] != true {
		t.Fatalf("Chat message refusal schema = %#v, want nullable", refusal)
	}
}

func TestOpenAPIRequestSchemasRequireRuntimeCoreFields(t *testing.T) {
	t.Setenv("LLMGW_BEARER_TOKEN", "snapshot-test-token")
	t.Setenv("OPENAI_API_KEY", "snapshot-openai-key")
	t.Setenv("ANTHROPIC_API_KEY", "snapshot-anthropic-key")
	t.Setenv("GEMINI_API_KEY", "snapshot-gemini-key")
	cfg, err := gateway.LoadConfigFile(filepath.Join("..", "..", "..", "config", "config.example.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	document := openAPIJSONDocument(t, cfg)
	for path, fields := range map[string][]string{
		"/v1/chat/completions":                         {"model", "messages"},
		"/v1/responses":                                {"model", "input"},
		"/v1/completions":                              {"model", "prompt"},
		"/v1/embeddings":                               {"model", "input"},
		"/v1/messages":                                 {"model", "messages", "max_tokens"},
		"/v1beta/models/{model}:generateContent":       {"contents"},
		"/v1beta/models/{model}:embedContent":          {"content"},
		"/v1beta/models/{model}:streamGenerateContent": {"contents"},
	} {
		schema := requestBodySchema(t, document, path)
		for _, field := range fields {
			if !schemaRequires(schema, field) {
				t.Fatalf("%s request schema does not require %q: %#v", path, field, schema)
			}
		}
	}

	responses := requestBodySchema(t, document, "/v1/responses")
	textSchema := schemaProperty(t, responses, "text")
	formatSchema := schemaProperty(t, textSchema, "format")
	formatProperties := objectField(t, formatSchema, "properties")
	for _, field := range []string{"type", "name", "schema", "strict"} {
		if _, ok := formatProperties[field]; !ok {
			t.Fatalf("Responses text.format is missing flat %q: %#v", field, formatProperties)
		}
	}
	if _, nested := formatProperties["json_schema"]; nested {
		t.Fatalf("Responses text.format incorrectly uses nested Chat json_schema: %#v", formatProperties)
	}
}

func TestOpenAPIBridgeEligibleModelsAreEnumerated(t *testing.T) {
	cfg := &gateway.Snapshot{
		Auth: gateway.AuthConfig{AllowAnonymous: true},
		Routes: map[string]*gateway.Route{
			"chat-only": {
				Name: "chat-only", Provider: "openai", Model: "bridge-alias", BaseURL: "https://example.test/v1",
				Capabilities: gateway.Capability{Operations: []gateway.Operation{gateway.OpChatCompletions}, Tokenizer: "cl100k_base"},
			},
		},
	}
	document := openAPIJSONDocument(t, cfg)
	for _, path := range []string{"/v1/responses", "/v1/completions"} {
		paths := objectField(t, document, "paths")
		post := objectField(t, objectField(t, paths, path), "post")
		requestBody := objectField(t, post, "requestBody")
		content := objectField(t, requestBody, "content")
		schema := objectField(t, objectField(t, content, "application/json"), "schema")
		model := schemaProperty(t, schema, "model")
		enum, ok := model["enum"].([]any)
		if !ok || len(enum) != 1 || enum[0] != "bridge-alias" {
			t.Fatalf("%s model enum = %#v, want bridge-alias", path, model["enum"])
		}
	}
}

func TestOpenAPIServerURLUsesPublicOrigin(t *testing.T) {
	document := openAPIJSONDocument(t, &gateway.Snapshot{
		Server: gateway.ServerConfig{Listen: "[::1]:8080"},
		Auth:   gateway.AuthConfig{AllowAnonymous: true},
	})
	servers, ok := document["servers"].([]any)
	if !ok || len(servers) != 1 {
		t.Fatalf("servers = %#v, want one", document["servers"])
	}
	server, ok := servers[0].(map[string]any)
	if !ok || server["url"] != "/" {
		t.Fatalf("server = %#v, want relative public-origin URL", servers[0])
	}
}

func openAPIJSONDocument(t *testing.T, cfg *gateway.Snapshot) map[string]any {
	t.Helper()
	generated, err := gwapi.OpenAPIJSON(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(generated, &document); err != nil {
		t.Fatal(err)
	}
	return document
}

func responseSchema(t *testing.T, document map[string]any, path, status, mediaType string) map[string]any {
	t.Helper()
	paths := objectField(t, document, "paths")
	post := objectField(t, objectField(t, paths, path), "post")
	responses := objectField(t, post, "responses")
	response := objectField(t, responses, status)
	content := objectField(t, response, "content")
	return objectField(t, objectField(t, content, mediaType), "schema")
}

func requestBodySchema(t *testing.T, document map[string]any, path string) map[string]any {
	t.Helper()
	paths := objectField(t, document, "paths")
	post := objectField(t, objectField(t, paths, path), "post")
	requestBody := objectField(t, post, "requestBody")
	content := objectField(t, requestBody, "content")
	return objectField(t, objectField(t, content, "application/json"), "schema")
}

func schemaRequires(schema map[string]any, field string) bool {
	if required, ok := schema["required"].([]any); ok {
		for _, value := range required {
			if value == field {
				return true
			}
		}
	}
	if allOf, ok := schema["allOf"].([]any); ok {
		for _, value := range allOf {
			if part, ok := value.(map[string]any); ok && schemaRequires(part, field) {
				return true
			}
		}
	}
	return false
}

func schemaProperty(t *testing.T, schema map[string]any, name string) map[string]any {
	t.Helper()
	if properties, ok := schema["properties"].(map[string]any); ok {
		if property, ok := properties[name].(map[string]any); ok {
			return property
		}
	}
	if allOf, ok := schema["allOf"].([]any); ok {
		for _, item := range allOf {
			part, ok := item.(map[string]any)
			if !ok {
				continue
			}
			if properties, ok := part["properties"].(map[string]any); ok {
				if property, ok := properties[name].(map[string]any); ok {
					return property
				}
			}
		}
	}
	t.Fatalf("schema property %q not found in %#v", name, schema)
	return nil
}

func defaultResponseExample(t *testing.T, document map[string]any, path string) map[string]any {
	t.Helper()
	paths := objectField(t, document, "paths")
	pathItem := objectField(t, paths, path)
	post := objectField(t, pathItem, "post")
	responses := objectField(t, post, "responses")
	defaultResponse := objectField(t, responses, "default")
	content := objectField(t, defaultResponse, "content")
	jsonContent := objectField(t, content, "application/json")
	return objectField(t, jsonContent, "example")
}

func objectField(t *testing.T, object map[string]any, key string) map[string]any {
	t.Helper()
	value, ok := object[key].(map[string]any)
	if !ok {
		t.Fatalf("field %q in %#v is not an object", key, object)
	}
	return value
}
