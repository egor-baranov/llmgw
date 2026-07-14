package api

import (
	"context"
	"encoding/json"
	"sort"
	"strings"

	"llmgw/gateway"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3gen"
	"github.com/samber/lo"
	"gopkg.in/yaml.v3"
)

const (
	// A relative URL keeps Swagger and generated clients on the public origin
	// that served the document instead of leaking the container bind address.
	gatewayServerURL = "/"

	openAIChatSSEExample = "data: {\"id\":\"chatcmpl_1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hello\"},\"finish_reason\":null}]}\n\n" +
		"data: [DONE]\n\n"
	openAICompletionSSEExample = "data: {\"id\":\"cmpl_1\",\"object\":\"text_completion\",\"choices\":[{\"index\":0,\"text\":\"hello\",\"logprobs\":null,\"finish_reason\":null}]}\n\n" +
		"data: [DONE]\n\n"
	openAIResponsesSSEExample = "event: response.created\n" +
		"data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"status\":\"in_progress\"}}\n\n" +
		"event: response.completed\n" +
		"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"status\":\"completed\"}}\n\n"
	anthropicSSEExample = "event: message_start\n" +
		"data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"usage\":{\"input_tokens\":1,\"output_tokens\":0}}}\n\n" +
		"event: message_stop\n" +
		"data: {\"type\":\"message_stop\"}\n\n"
	geminiSSEExample = "data: {\"candidates\":[{\"index\":0,\"content\":{\"parts\":[{\"text\":\"hello\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":1,\"candidatesTokenCount\":1,\"totalTokenCount\":2}}\n\n"
)

type healthzResponseDoc struct {
	OK bool `json:"ok"`
}

type readyzResponseDoc struct {
	Ready bool `json:"ready"`
}

type errorObjectDoc struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Param   string `json:"param,omitempty"`
	Code    string `json:"code,omitempty"`
}

type errorEnvelopeDoc struct {
	Error errorObjectDoc `json:"error"`
}

type anthropicErrorObjectDoc struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

type anthropicErrorEnvelopeDoc struct {
	Type  string                  `json:"type"`
	Error anthropicErrorObjectDoc `json:"error"`
}

type geminiErrorObjectDoc struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Status  string `json:"status"`
}

type geminiErrorEnvelopeDoc struct {
	Error geminiErrorObjectDoc `json:"error"`
}

type modelListResponseDoc struct {
	Object string                    `json:"object"`
	Data   []gateway.ModelDescriptor `json:"data"`
}

type limitsResponseDoc struct {
	KeyID            string              `json:"key_id"`
	Source           string              `json:"source"`
	Limits           gateway.LimitSpec   `json:"limits"`
	Usage            *gateway.QuotaUsage `json:"usage,omitempty"`
	UsageUnavailable bool                `json:"usage_unavailable,omitempty"`
}

type inputAudioDoc struct {
	Data   string `json:"data,omitempty"`
	Format string `json:"format,omitempty"`
}

type contentPartDoc struct {
	Type        string          `json:"type,omitempty"`
	Text        string          `json:"text,omitempty"`
	Refusal     string          `json:"refusal,omitempty"`
	ImageURL    string          `json:"image_url,omitempty"`
	InputText   string          `json:"input_text,omitempty"`
	InputAudio  *inputAudioDoc  `json:"input_audio,omitempty"`
	FileID      string          `json:"file_id,omitempty"`
	MIMEType    string          `json:"mime_type,omitempty"`
	Annotations json.RawMessage `json:"annotations,omitempty"`
}

type toolDoc struct {
	Type        string          `json:"type,omitempty"`
	Name        string          `json:"name,omitempty"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
	Strict      bool            `json:"strict,omitempty"`
	Function    json.RawMessage `json:"function,omitempty"`
}

type toolCallDoc struct {
	ID        string          `json:"id,omitempty"`
	Type      string          `json:"type,omitempty"`
	Name      string          `json:"name,omitempty"`
	Arguments string          `json:"arguments,omitempty"`
	Function  json.RawMessage `json:"function,omitempty"`
}

type jsonSchemaDoc struct {
	Name        string          `json:"name,omitempty"`
	Description string          `json:"description,omitempty"`
	Schema      json.RawMessage `json:"schema,omitempty"`
	Strict      bool            `json:"strict,omitempty"`
}

type responseFormatDoc struct {
	Type       string          `json:"type,omitempty"`
	JSONSchema *jsonSchemaDoc  `json:"json_schema,omitempty"`
	Function   json.RawMessage `json:"function,omitempty"`
}

type reasoningDoc struct {
	Effort    string `json:"effort,omitempty"`
	MaxTokens int    `json:"max_tokens,omitempty"`
	Summary   string `json:"summary,omitempty"`
}

type messageDoc struct {
	Role       string            `json:"role,omitempty"`
	Content    json.RawMessage   `json:"content,omitempty"`
	Name       string            `json:"name,omitempty"`
	ToolCallID string            `json:"tool_call_id,omitempty"`
	ToolCalls  []toolCallDoc     `json:"tool_calls,omitempty"`
	Metadata   map[string]string `json:"metadata,omitempty"`
	Refusal    json.RawMessage   `json:"refusal,omitempty"`
}

type responseOutputDoc struct {
	Type      string           `json:"type,omitempty"`
	ID        string           `json:"id,omitempty"`
	Status    string           `json:"status,omitempty"`
	Role      string           `json:"role,omitempty"`
	Content   []contentPartDoc `json:"content,omitempty"`
	CallID    string           `json:"call_id,omitempty"`
	Name      string           `json:"name,omitempty"`
	Arguments string           `json:"arguments,omitempty"`
}

type embeddingDataDoc struct {
	Object    string    `json:"object,omitempty"`
	Index     int       `json:"index,omitempty"`
	Embedding []float64 `json:"embedding,omitempty"`
}

type chatRequestDoc struct {
	Messages            []messageDoc       `json:"messages"`
	Tools               []toolDoc          `json:"tools,omitempty"`
	ToolChoice          json.RawMessage    `json:"tool_choice,omitempty"`
	ResponseFormat      *responseFormatDoc `json:"response_format,omitempty"`
	Metadata            map[string]string  `json:"metadata,omitempty"`
	Reasoning           *reasoningDoc      `json:"reasoning,omitempty"`
	MaxTokens           int                `json:"max_tokens,omitempty"`
	MaxCompletionTokens int                `json:"max_completion_tokens,omitempty"`
	MaxOutputTokens     int                `json:"max_output_tokens,omitempty"`
	N                   int                `json:"n,omitempty"`
	User                string             `json:"user,omitempty"`
}

type responsesTextDoc struct {
	Format *responsesFormatDoc `json:"format,omitempty"`
}

type responsesFormatDoc struct {
	Type        string          `json:"type,omitempty"`
	Name        string          `json:"name,omitempty"`
	Description string          `json:"description,omitempty"`
	Schema      json.RawMessage `json:"schema,omitempty"`
	Strict      bool            `json:"strict,omitempty"`
}

type responsesRequestDoc struct {
	Input           json.RawMessage   `json:"input"`
	Instructions    string            `json:"instructions,omitempty"`
	Tools           []toolDoc         `json:"tools,omitempty"`
	ToolChoice      json.RawMessage   `json:"tool_choice,omitempty"`
	Text            *responsesTextDoc `json:"text,omitempty"`
	Metadata        map[string]string `json:"metadata,omitempty"`
	Reasoning       *reasoningDoc     `json:"reasoning,omitempty"`
	MaxOutputTokens int               `json:"max_output_tokens,omitempty"`
	User            string            `json:"user,omitempty"`
}

type completionRequestDoc struct {
	Prompt    json.RawMessage   `json:"prompt"`
	Suffix    string            `json:"suffix,omitempty"`
	MaxTokens int               `json:"max_tokens,omitempty"`
	N         int               `json:"n,omitempty"`
	BestOf    int               `json:"best_of,omitempty"`
	User      string            `json:"user,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

type embeddingRequestDoc struct {
	Input          json.RawMessage   `json:"input"`
	EncodingFormat string            `json:"encoding_format,omitempty"`
	Dimensions     int               `json:"dimensions,omitempty"`
	User           string            `json:"user,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
}

type chatChoiceDoc struct {
	Index        int             `json:"index,omitempty"`
	Message      messageDoc      `json:"message,omitempty"`
	FinishReason string          `json:"finish_reason,omitempty"`
	Logprobs     json.RawMessage `json:"logprobs,omitempty"`
}

type chatResponseDoc struct {
	ID      string          `json:"id,omitempty"`
	Object  string          `json:"object,omitempty"`
	Model   string          `json:"model,omitempty"`
	Created int64           `json:"created,omitempty"`
	Choices []chatChoiceDoc `json:"choices,omitempty"`
	Usage   chatUsageDoc    `json:"usage,omitempty"`
}

type chatUsageDoc struct {
	PromptTokens            int64           `json:"prompt_tokens,omitempty"`
	CompletionTokens        int64           `json:"completion_tokens,omitempty"`
	TotalTokens             int64           `json:"total_tokens,omitempty"`
	PromptTokensDetails     json.RawMessage `json:"prompt_tokens_details,omitempty"`
	CompletionTokensDetails json.RawMessage `json:"completion_tokens_details,omitempty"`
}

type responsesResponseDoc struct {
	ID                string                `json:"id,omitempty"`
	Object            string                `json:"object,omitempty"`
	Model             string                `json:"model,omitempty"`
	CreatedAt         int64                 `json:"created_at,omitempty"`
	Status            string                `json:"status,omitempty"`
	Output            []responseOutputDoc   `json:"output,omitempty"`
	OutputText        string                `json:"output_text,omitempty"`
	IncompleteDetails *incompleteDetailsDoc `json:"incomplete_details,omitempty"`
	Usage             responsesUsageDoc     `json:"usage,omitempty"`
}

type incompleteDetailsDoc struct {
	Reason string `json:"reason,omitempty"`
}

type responsesUsageDoc struct {
	InputTokens   int64           `json:"input_tokens,omitempty"`
	OutputTokens  int64           `json:"output_tokens,omitempty"`
	TotalTokens   int64           `json:"total_tokens,omitempty"`
	InputDetails  json.RawMessage `json:"input_tokens_details,omitempty"`
	OutputDetails json.RawMessage `json:"output_tokens_details,omitempty"`
}

type completionChoiceDoc struct {
	Index        int             `json:"index,omitempty"`
	Text         string          `json:"text,omitempty"`
	Logprobs     json.RawMessage `json:"logprobs,omitempty"`
	FinishReason string          `json:"finish_reason,omitempty"`
}

type completionResponseDoc struct {
	ID      string                `json:"id,omitempty"`
	Object  string                `json:"object,omitempty"`
	Model   string                `json:"model,omitempty"`
	Created int64                 `json:"created,omitempty"`
	Choices []completionChoiceDoc `json:"choices,omitempty"`
	Usage   chatUsageDoc          `json:"usage,omitempty"`
}

type embeddingsResponseDoc struct {
	Object string             `json:"object,omitempty"`
	Model  string             `json:"model,omitempty"`
	Data   []embeddingDataDoc `json:"data,omitempty"`
	Usage  gateway.Usage      `json:"usage,omitempty"`
}

type schemaSet map[string]*openapi3.SchemaRef

const (
	schemaHealthz        = "Healthz"
	schemaReadyz         = "Readyz"
	schemaErrorEnvelope  = "ErrorEnvelope"
	schemaAnthropicError = "AnthropicErrorEnvelope"
	schemaGeminiError    = "GeminiErrorEnvelope"
	schemaModel          = "Model"
	schemaModelList      = "ModelList"
	schemaInputAudio     = "InputAudio"
	schemaContentPart    = "ContentPart"
	schemaTool           = "Tool"
	schemaToolCall       = "ToolCall"
	schemaJSONSchema     = "JSONSchema"
	schemaResponseFmt    = "ResponseFormat"
	schemaReasoning      = "Reasoning"
	schemaMessage        = "Message"
	schemaUsage          = "Usage"
	schemaDuration       = "Duration"
	schemaQuotaUsage     = "QuotaUsage"
	schemaLimitSpec      = "LimitSpec"
	schemaQuotaLimits    = "QuotaLimitsResponse"
	schemaEmbeddingData  = "EmbeddingData"
	schemaResponseOut    = "ResponseOutput"
	schemaChatReq        = "ChatRequest"
	schemaResponsesReq   = "ResponsesRequest"
	schemaCompletionReq  = "CompletionRequest"
	schemaEmbeddingReq   = "EmbeddingRequest"
	schemaChatResp       = "ChatResponse"
	schemaResponsesResp  = "ResponsesResponse"
	schemaCompletionRes  = "CompletionResponse"
	schemaEmbeddingRes   = "EmbeddingsResponse"
)

type schemaSpec struct {
	key   string
	value any
}

type pathOperation struct {
	path string
	op   *openapi3.Operation
}

func OpenAPIYAML(cfg *gateway.Snapshot) ([]byte, error) {
	doc, err := buildOpenAPIDoc(cfg)
	if err != nil {
		return nil, err
	}
	value, err := doc.MarshalYAML()
	if err != nil {
		return nil, err
	}
	return yaml.Marshal(value)
}

func OpenAPIJSON(cfg *gateway.Snapshot) ([]byte, error) {
	doc, err := buildOpenAPIDoc(cfg)
	if err != nil {
		return nil, err
	}
	return json.Marshal(doc)
}

func buildOpenAPIDoc(cfg *gateway.Snapshot) (*openapi3.T, error) {
	models := modelEnum(cfg)
	exampleModel := "gpt-4o-mini"
	if len(models) > 0 {
		exampleModel = models[0]
	}

	doc := &openapi3.T{
		OpenAPI: "3.0.3",
		Info: &openapi3.Info{
			Title:       "llmgw",
			Version:     "v1",
			Description: "Compact provider-native LLM gateway with provider-aware routing, capability filtering, quota enforcement, SSE streaming, and provider-specific passthrough fields.",
		},
		Servers: openapi3.Servers{
			&openapi3.Server{URL: gatewayServerURL, Description: "Configured gateway listener"},
		},
		Tags: openapi3.Tags{
			&openapi3.Tag{Name: "system", Description: "Health, readiness, metrics, and discovery endpoints."},
			&openapi3.Tag{Name: "provider", Description: "Provider-native ingress endpoints. Requests are routed only to matching providers and are not translated across provider families."},
			&openapi3.Tag{Name: "quota", Description: "JWT or token self-service quota management keyed by the authenticated quota identifier."},
			&openapi3.Tag{Name: "docs", Description: "Generated spec and interactive docs."},
		},
		Paths: openapi3.NewPaths(),
		Components: &openapi3.Components{
			Schemas: openapi3.Schemas{},
			SecuritySchemes: openapi3.SecuritySchemes{
				"bearerAuth": &openapi3.SecuritySchemeRef{
					Value: openapi3.NewSecurityScheme().
						WithType("http").
						WithScheme("bearer").
						WithBearerFormat("API key").
						WithDescription("Send Authorization: Bearer <token> when auth is enabled."),
				},
				"anthropicGatewayKey": &openapi3.SecuritySchemeRef{Value: openapi3.NewSecurityScheme().
					WithType("apiKey").WithIn("header").WithName("x-api-key").
					WithDescription("Gateway token on Anthropic-native ingress paths.")},
				"geminiGatewayKey": &openapi3.SecuritySchemeRef{Value: openapi3.NewSecurityScheme().
					WithType("apiKey").WithIn("header").WithName("x-goog-api-key").
					WithDescription("Gateway token on Gemini-native ingress paths.")},
			},
		},
	}

	refs, err := buildGeneratedSchemas(doc.Components.Schemas)
	if err != nil {
		return nil, err
	}
	patchGeneratedSchemas(doc.Components.Schemas, refs)
	inlineGeneratedComponentRefs(doc.Components.Schemas)

	security := inferenceSecurity(cfg)
	doc.Paths.Set("/healthz", &openapi3.PathItem{Get: operation("healthz", "Liveness probe", "", []string{"system"}, nil, responses(
		responseStatus("200", jsonResponse(refs.ref(schemaHealthz), "Gateway process is alive.", map[string]any{"ok": true})),
	))})
	doc.Paths.Set("/readyz", &openapi3.PathItem{Get: operation("readyz", "Readiness probe", "", []string{"system"}, nil, responses(
		responseStatus("200", jsonResponse(refs.ref(schemaReadyz), "Configuration is loaded and ready.", map[string]any{"ready": true})),
		responseStatus("503", jsonResponse(refs.ref(schemaErrorEnvelope), "Configuration has not been loaded yet.", map[string]any{"error": map[string]any{"message": "configuration not loaded", "type": "server_error", "code": "not_ready"}})),
	))})
	doc.Paths.Set("/metrics", &openapi3.PathItem{Get: operation("metrics", "Prometheus metrics", "", []string{"system"}, nil, responses(
		responseStatus("200", responseWithContent("Prometheus exposition format.", openapi3.Content{
			"text/plain": media(stringSchemaRef(), nil),
		})),
	), withSecurity(security))})
	doc.Paths.Set("/openapi.yaml", &openapi3.PathItem{Get: operation("openapiYAML", "Generated OpenAPI YAML", "", []string{"docs"}, nil, responses(
		responseStatus("200", responseWithContent("Live OpenAPI document for the gateway surface.", openapi3.Content{
			"application/yaml": media(stringSchemaRef(), nil),
			"text/yaml":        media(stringSchemaRef(), nil),
		})),
	), withSecurity(security))})
	doc.Paths.Set("/openapi.json", &openapi3.PathItem{Get: operation("openapiJSON", "Generated OpenAPI JSON", "", []string{"docs"}, nil, responses(
		responseStatus("200", responseWithContent("Live OpenAPI document for the gateway surface.", openapi3.Content{
			"application/json": media(objectAnySchemaRef(), nil),
		})),
	), withSecurity(security))})
	doc.Paths.Set("/docs", &openapi3.PathItem{Get: operation("docsRedirect", "Redirect to Swagger UI", "", []string{"docs"}, nil, responses(
		responseStatus("301", responseWithContent("Redirects to /docs/index.html.", nil)),
	), withSecurity(security))})
	doc.Paths.Set("/docs/index.html", &openapi3.PathItem{Get: operation("docs", "Swagger UI", "", []string{"docs"}, nil, responses(
		responseStatus("200", responseWithContent("Swagger UI backed by /openapi.json.", openapi3.Content{
			"text/html": media(stringSchemaRef(), nil),
		})),
	), withSecurity(security))})
	doc.Paths.Set("/v1/models", &openapi3.PathItem{Get: operation("listModels", "List accessible route models", "Returns only models allowed by the authenticated principal's model and provider ACLs.", []string{"system"}, nil, responses(
		responseStatus("200", jsonResponse(refs.ref(schemaModelList), "Models visible to the caller.", map[string]any{
			"object": "list",
			"data":   []map[string]any{{"id": exampleModel, "object": "model", "owned_by": "llmgw"}},
		})),
		responseStatus("default", jsonResponse(refs.ref(schemaErrorEnvelope), "Error response.", map[string]any{"error": map[string]any{"message": "request failed", "type": "invalid_request_error"}})),
	), withSecurity(security))})
	doc.Paths.Set("/v1/limits", &openapi3.PathItem{
		Get: operation("getLimits", "Get quota limits for the authenticated token key", "", []string{"quota"}, nil, responses(
			responseStatus("200", jsonResponse(refs.ref(schemaQuotaLimits), "Resolved quota limits and current usage for the authenticated key.", map[string]any{
				"key_id": "demo-key",
				"source": "dynamic",
				"limits": map[string]any{"rpm": 60, "daily_tokens": 1000000},
				"usage":  map[string]any{"rpm_current": 3, "daily_used_tokens": 240},
			})),
			responseStatus("404", jsonResponse(refs.ref(schemaErrorEnvelope), "No quota limits are configured for the authenticated key.", map[string]any{"error": map[string]any{"message": "no quota limits configured for this token", "type": "invalid_request_error", "code": "quota_limits_not_found"}})),
			responseStatus("default", jsonResponse(refs.ref(schemaErrorEnvelope), "Error response.", map[string]any{"error": map[string]any{"message": "unauthorized", "type": "invalid_request_error"}})),
		), withSecurity(openapi3.NewSecurityRequirements().With(openapi3.NewSecurityRequirement().Authenticate("bearerAuth")))),
		Put: operation("putLimits", "Set quota limits for the authenticated token key", "Requires the manage_limits principal permission.", []string{"quota"}, requestBodyRef(refs.ref(schemaLimitSpec), map[string]any{
			"rpm":              120,
			"tpm":              500000,
			"max_parallel":     8,
			"daily_tokens":     2000000,
			"max_spend_micros": 5000000,
		}), responses(
			responseStatus("200", jsonResponse(refs.ref(schemaQuotaLimits), "Stored quota limits for the authenticated key.", map[string]any{
				"key_id": "demo-key",
				"source": "dynamic",
				"limits": map[string]any{"rpm": 120, "daily_tokens": 2000000},
			})),
			responseStatus("default", jsonResponse(refs.ref(schemaErrorEnvelope), "Error response.", map[string]any{"error": map[string]any{"message": "invalid request", "type": "invalid_request_error"}})),
		), withSecurity(openapi3.NewSecurityRequirements().With(openapi3.NewSecurityRequirement().Authenticate("bearerAuth")))),
	})
	for _, item := range providerOperations(refs, cfg, exampleModel, security) {
		doc.Paths.Set(item.path, &openapi3.PathItem{Post: item.op})
	}

	if err := doc.Validate(context.Background()); err != nil {
		return nil, err
	}
	return doc, nil
}

func buildGeneratedSchemas(schemas openapi3.Schemas) (schemaSet, error) {
	generator := openapi3gen.NewGenerator(
		openapi3gen.CreateComponentSchemas(openapi3gen.ExportComponentSchemasOptions{
			ExportComponentSchemas: true,
			ExportTopLevelSchema:   true,
		}),
	)
	specs := []schemaSpec{
		{schemaHealthz, healthzResponseDoc{}},
		{schemaReadyz, readyzResponseDoc{}},
		{schemaErrorEnvelope, errorEnvelopeDoc{}},
		{schemaAnthropicError, anthropicErrorEnvelopeDoc{}},
		{schemaGeminiError, geminiErrorEnvelopeDoc{}},
		{schemaModel, gateway.ModelDescriptor{}},
		{schemaModelList, modelListResponseDoc{}},
		{schemaInputAudio, inputAudioDoc{}},
		{schemaContentPart, contentPartDoc{}},
		{schemaTool, toolDoc{}},
		{schemaToolCall, toolCallDoc{}},
		{schemaJSONSchema, jsonSchemaDoc{}},
		{schemaResponseFmt, responseFormatDoc{}},
		{schemaReasoning, reasoningDoc{}},
		{schemaMessage, messageDoc{}},
		{schemaUsage, gateway.Usage{}},
		{schemaDuration, gateway.Duration{}},
		{schemaQuotaUsage, gateway.QuotaUsage{}},
		{schemaLimitSpec, gateway.LimitSpec{}},
		{schemaQuotaLimits, limitsResponseDoc{}},
		{schemaEmbeddingData, embeddingDataDoc{}},
		{schemaResponseOut, responseOutputDoc{}},
		{schemaChatReq, chatRequestDoc{}},
		{schemaResponsesReq, responsesRequestDoc{}},
		{schemaCompletionReq, completionRequestDoc{}},
		{schemaEmbeddingReq, embeddingRequestDoc{}},
		{schemaChatResp, chatResponseDoc{}},
		{schemaResponsesResp, responsesResponseDoc{}},
		{schemaCompletionRes, completionResponseDoc{}},
		{schemaEmbeddingRes, embeddingsResponseDoc{}},
	}
	refs := make(schemaSet, len(specs))
	for _, spec := range specs {
		ref, err := generator.NewSchemaRefForValue(spec.value, schemas)
		if err != nil {
			return nil, err
		}
		refs[spec.key] = ref
	}
	inlineTopLevelSchemaRefs(schemas, refs)
	return refs, nil
}

func patchGeneratedSchemas(schemas openapi3.Schemas, refs schemaSet) {
	for _, key := range []string{
		schemaContentPart,
		schemaTool,
		schemaToolCall,
		schemaJSONSchema,
		schemaResponseFmt,
		schemaReasoning,
		schemaMessage,
		schemaChatReq,
		schemaResponsesReq,
		schemaCompletionReq,
		schemaEmbeddingReq,
	} {
		setAnyAdditionalProperties(schemaFor(schemas, refs.ref(key)))
	}

	addRequired(schemaFor(schemas, refs.ref(schemaChatReq)), "messages")
	addRequired(schemaFor(schemas, refs.ref(schemaResponsesReq)), "input")
	addRequired(schemaFor(schemas, refs.ref(schemaCompletionReq)), "prompt")
	addRequired(schemaFor(schemas, refs.ref(schemaEmbeddingReq)), "input")
	setProperty(schemaFor(schemas, refs.ref(schemaTool)), "parameters", objectAnySchemaRef())
	setProperty(schemaFor(schemas, refs.ref(schemaTool)), "function", objectAnySchemaRef())
	setProperty(schemaFor(schemas, refs.ref(schemaToolCall)), "function", objectAnySchemaRef())
	setProperty(schemaFor(schemas, refs.ref(schemaJSONSchema)), "schema", objectAnySchemaRef())
	setProperty(schemaFor(schemas, refs.ref(schemaContentPart)), "image_url", stringOrObjectSchemaRef())
	setProperty(schemaFor(schemas, refs.ref(schemaContentPart)), "annotations", arraySchemaRef(objectAnySchemaRef()))
	setProperty(schemaFor(schemas, refs.ref(schemaChatReq)), "tool_choice", stringOrObjectSchemaRef())
	setProperty(schemaFor(schemas, refs.ref(schemaResponsesReq)), "tool_choice", stringOrObjectSchemaRef())
	setProperty(schemaFor(schemas, refs.ref(schemaLimitSpec)), "budget_duration", stringSchemaRef())
	setProperty(schemaFor(schemas, refs.ref(schemaMessage)), "content", contentUnionSchemaRef(refs.ref(schemaContentPart)))
	setProperty(schemaFor(schemas, refs.ref(schemaMessage)), "refusal", nullableStringSchemaRef())
	setProperty(schemaFor(schemas, refs.ref(schemaResponsesReq)), "input", responsesInputSchemaRef(refs.ref(schemaMessage), refs.ref(schemaContentPart)))
	setProperty(schemaFor(schemas, refs.ref(schemaCompletionReq)), "prompt", textInputSchemaRef())
	setProperty(schemaFor(schemas, refs.ref(schemaEmbeddingReq)), "input", textInputSchemaRef())
	completion := schemaFor(schemas, refs.ref(schemaCompletionRes))
	if completion != nil {
		choices := schemaFor(schemas, completion.Properties["choices"])
		if choices != nil {
			choice := schemaFor(schemas, choices.Items)
			setProperty(choice, "logprobs", nullableObjectAnySchemaRef())
		}
	}
	chat := schemaFor(schemas, refs.ref(schemaChatResp))
	if chat != nil {
		choices := schemaFor(schemas, chat.Properties["choices"])
		if choices != nil {
			choice := schemaFor(schemas, choices.Items)
			setProperty(choice, "logprobs", nullableObjectAnySchemaRef())
		}
	}
}

func addRequired(schema *openapi3.Schema, fields ...string) {
	if schema == nil {
		return
	}
	seen := make(map[string]struct{}, len(schema.Required)+len(fields))
	for _, field := range schema.Required {
		seen[field] = struct{}{}
	}
	for _, field := range fields {
		if _, ok := seen[field]; ok {
			continue
		}
		schema.Required = append(schema.Required, field)
		seen[field] = struct{}{}
	}
}

func inlineTopLevelSchemaRefs(schemas openapi3.Schemas, refs schemaSet) {
	for key, ref := range refs {
		refs[key] = inlineComponentSchemaRef(schemas, ref)
	}
}

func operation(id, summary, description string, tags []string, body *openapi3.RequestBodyRef, responses *openapi3.Responses, options ...func(*openapi3.Operation)) *openapi3.Operation {
	op := openapi3.NewOperation()
	op.OperationID = id
	op.Summary = summary
	op.Description = description
	op.Tags = tags
	op.RequestBody = body
	op.Responses = responses
	for _, option := range options {
		option(op)
	}
	return op
}

func withSecurity(security *openapi3.SecurityRequirements) func(*openapi3.Operation) {
	return func(op *openapi3.Operation) {
		if security != nil {
			op.Security = security
		}
	}
}

func withPathParameter(name, description string, example any) func(*openapi3.Operation) {
	return func(op *openapi3.Operation) {
		schema := openapi3.NewStringSchema()
		schema.Description = description
		schema.Example = example
		op.Parameters = append(op.Parameters, &openapi3.ParameterRef{
			Value: &openapi3.Parameter{
				In:       "path",
				Name:     name,
				Required: true,
				Schema:   openapi3.NewSchemaRef("", schema),
			},
		})
	}
}

func requestBodyRef(schema *openapi3.SchemaRef, example any) *openapi3.RequestBodyRef {
	body := openapi3.NewRequestBody().WithRequired(true)
	body.Content = openapi3.Content{
		"application/json": media(schema, example),
	}
	return &openapi3.RequestBodyRef{Value: body}
}

func responses(items ...func(*openapi3.Responses)) *openapi3.Responses {
	out := openapi3.NewResponsesWithCapacity(len(items))
	for _, item := range items {
		item(out)
	}
	return out
}

func providerOperations(refs schemaSet, cfg *gateway.Snapshot, defaultExample string, security *openapi3.SecurityRequirements) []pathOperation {
	anthropicSecurity := alternativeSecurity(security, "anthropicGatewayKey")
	geminiSecurity := alternativeSecurity(security, "geminiGatewayKey")
	chatModels := providerModelEnum(cfg, "openai", gateway.OpChatCompletions)
	responseModels := providerModelEnum(cfg, "openai", gateway.OpResponses, gateway.OpChatCompletions)
	completionModels := providerModelEnum(cfg, "openai", gateway.OpCompletions, gateway.OpChatCompletions)
	embeddingModels := providerModelEnum(cfg, "openai", gateway.OpEmbeddings)
	chatExample := firstModelOr(chatModels, defaultExample)
	responseExample := firstModelOr(responseModels, chatExample)
	completionExample := firstModelOr(completionModels, chatExample)
	embeddingExample := firstModelOr(embeddingModels, chatExample)
	anthropicExample := firstModelOr(providerModelEnum(cfg, "anthropic", gateway.OpChatCompletions), defaultExample)
	geminiChatExample := firstModelOr(providerModelEnum(cfg, "gemini", gateway.OpChatCompletions), defaultExample)
	geminiEmbeddingExample := firstModelOr(providerModelEnum(cfg, "gemini", gateway.OpEmbeddings), geminiChatExample)
	return []pathOperation{
		{
			path: "/v1/chat/completions",
			op: operation(
				"openAIChatCompletions",
				"OpenAI chat completions",
				"OpenAI-native endpoint. Requests are routed only to OpenAI routes and are not translated across provider families.",
				[]string{"provider"},
				requestBodyRef(requestSchema(refs.ref(schemaChatReq), chatModels, true), map[string]any{
					"model": chatExample,
					"messages": []map[string]any{{
						"role":    "user",
						"content": "Say hello in one sentence.",
					}},
				}),
				responses(
					responseStatus("200", responseWithContent("OpenAI-native chat completion or SSE stream.", openapi3.Content{
						"application/json":  media(refs.ref(schemaChatResp), nil),
						"text/event-stream": media(stringSchemaRef(), openAIChatSSEExample),
					})),
					responseStatus("default", openAIErrorResponse(refs)),
				),
				withSecurity(security),
			),
		},
		{
			path: "/v1/responses",
			op: operation(
				"openAIResponses",
				"OpenAI responses",
				"OpenAI endpoint. Responses-native routes are preferred; unary requests may use a loss-checked chat-completions compatibility bridge. Requests are never translated across provider families.",
				[]string{"provider"},
				requestBodyRef(requestSchema(refs.ref(schemaResponsesReq), responseModels, true), map[string]any{
					"model": responseExample,
					"input": "Summarize the gateway in one line.",
				}),
				responses(
					responseStatus("200", responseWithContent("OpenAI-native response JSON or SSE stream.", openapi3.Content{
						"application/json":  media(refs.ref(schemaResponsesResp), nil),
						"text/event-stream": media(stringSchemaRef(), openAIResponsesSSEExample),
					})),
					responseStatus("default", openAIErrorResponse(refs)),
				),
				withSecurity(security),
			),
		},
		{
			path: "/v1/completions",
			op: operation(
				"openAICompletions",
				"OpenAI legacy completions",
				"OpenAI endpoint. Completions-native routes are preferred; loss-checked text-only requests, including streams, may use a chat-completions compatibility bridge. Requests are never translated across provider families.",
				[]string{"provider"},
				requestBodyRef(requestSchema(refs.ref(schemaCompletionReq), completionModels, true), map[string]any{
					"model":  completionExample,
					"prompt": "Write one short sentence about Go.",
				}),
				responses(
					responseStatus("200", responseWithContent("OpenAI-native completion JSON or SSE stream.", openapi3.Content{
						"application/json":  media(refs.ref(schemaCompletionRes), nil),
						"text/event-stream": media(stringSchemaRef(), openAICompletionSSEExample),
					})),
					responseStatus("default", openAIErrorResponse(refs)),
				),
				withSecurity(security),
			),
		},
		{
			path: "/v1/embeddings",
			op: operation(
				"openAIEmbeddings",
				"OpenAI embeddings",
				"OpenAI-native endpoint. Requests are routed only to OpenAI routes and are not translated across provider families.",
				[]string{"provider"},
				requestBodyRef(requestSchema(refs.ref(schemaEmbeddingReq), embeddingModels, false), map[string]any{
					"model": embeddingExample,
					"input": "gateway",
				}),
				responses(
					responseStatus("200", jsonResponse(refs.ref(schemaEmbeddingRes), "OpenAI-native embeddings response.", map[string]any{
						"object": "list",
						"model":  embeddingExample,
						"data":   []map[string]any{{"object": "embedding", "index": 0, "embedding": []float64{0.1, 0.2}}},
						"usage":  map[string]any{"prompt_tokens": 1, "total_tokens": 1},
					})),
					responseStatus("default", openAIErrorResponse(refs)),
				),
				withSecurity(security),
			),
		},
		{
			path: "/v1/messages",
			op: operation(
				"anthropicMessages",
				"Anthropic messages",
				"Anthropic-native endpoint. Requests are routed only to Anthropic routes and are not translated across provider families.",
				[]string{"provider"},
				requestBodyRef(anthropicRequestSchemaRef(), map[string]any{
					"model":      anthropicExample,
					"max_tokens": 1024,
					"messages": []map[string]any{{
						"role":    "user",
						"content": "Say hello in one sentence.",
					}},
				}),
				responses(
					responseStatus("200", responseWithContent("Anthropic-native message JSON or SSE stream.", openapi3.Content{
						"application/json": media(objectAnySchemaRef(), map[string]any{
							"id": "msg_1", "type": "message", "role": "assistant", "model": anthropicExample,
						}),
						"text/event-stream": media(stringSchemaRef(), anthropicSSEExample),
					})),
					responseStatus("default", anthropicErrorResponse(refs)),
				),
				withSecurity(anthropicSecurity),
			),
		},
		{
			path: "/v1beta/models/{model}:generateContent",
			op: operation(
				"geminiGenerateContentV1Beta",
				"Gemini generateContent (v1beta)",
				"Gemini-native endpoint. Requests are routed only to Gemini routes and are not translated across provider families.",
				[]string{"provider"},
				requestBodyRef(geminiGenerateRequestSchemaRef(), map[string]any{
					"contents": []map[string]any{{
						"role":  "user",
						"parts": []map[string]any{{"text": "Say hello in one sentence."}},
					}},
				}),
				responses(
					responseStatus("200", jsonResponse(objectAnySchemaRef(), "Gemini-native generateContent response.", map[string]any{
						"responseId": "resp_1",
						"candidates": []map[string]any{{"index": 0}},
					})),
					responseStatus("default", geminiErrorResponse(refs)),
				),
				withPathParameter("model", "Gemini model id from the native URL path.", geminiChatExample),
				withSecurity(geminiSecurity),
			),
		},
		{
			path: "/v1beta/models/{model}:embedContent",
			op: operation(
				"geminiEmbedContentV1Beta",
				"Gemini embedContent (v1beta)",
				"Gemini-native endpoint. Requests are routed only to Gemini routes and are not translated across provider families.",
				[]string{"provider"},
				requestBodyRef(geminiEmbeddingRequestSchemaRef(), map[string]any{
					"content": map[string]any{
						"parts": []map[string]any{{"text": "gateway"}},
					},
				}),
				responses(
					responseStatus("200", jsonResponse(objectAnySchemaRef(), "Gemini-native embedContent response.", map[string]any{
						"embedding":     map[string]any{"values": []float64{0.1, 0.2}},
						"usageMetadata": map[string]any{"promptTokenCount": 1, "totalTokenCount": 1},
					})),
					responseStatus("default", geminiErrorResponse(refs)),
				),
				withPathParameter("model", "Gemini model id from the native URL path.", geminiEmbeddingExample),
				withSecurity(geminiSecurity),
			),
		},
		{
			path: "/v1/models/{model}:generateContent",
			op: operation(
				"geminiGenerateContentV1",
				"Gemini generateContent (v1)",
				"Gemini-native endpoint. Requests are routed only to Gemini routes and are not translated across provider families.",
				[]string{"provider"},
				requestBodyRef(geminiGenerateRequestSchemaRef(), map[string]any{
					"contents": []map[string]any{{
						"role":  "user",
						"parts": []map[string]any{{"text": "Say hello in one sentence."}},
					}},
				}),
				responses(
					responseStatus("200", jsonResponse(objectAnySchemaRef(), "Gemini-native generateContent response.", map[string]any{
						"responseId": "resp_1",
						"candidates": []map[string]any{{"index": 0}},
					})),
					responseStatus("default", geminiErrorResponse(refs)),
				),
				withPathParameter("model", "Gemini model id from the native URL path.", geminiChatExample),
				withSecurity(geminiSecurity),
			),
		},
		{
			path: "/v1/models/{model}:embedContent",
			op: operation(
				"geminiEmbedContentV1",
				"Gemini embedContent (v1)",
				"Gemini-native endpoint. Requests are routed only to Gemini routes and are not translated across provider families.",
				[]string{"provider"},
				requestBodyRef(geminiEmbeddingRequestSchemaRef(), map[string]any{
					"content": map[string]any{
						"parts": []map[string]any{{"text": "gateway"}},
					},
				}),
				responses(
					responseStatus("200", jsonResponse(objectAnySchemaRef(), "Gemini-native embedContent response.", map[string]any{
						"embedding":     map[string]any{"values": []float64{0.1, 0.2}},
						"usageMetadata": map[string]any{"promptTokenCount": 1, "totalTokenCount": 1},
					})),
					responseStatus("default", geminiErrorResponse(refs)),
				),
				withPathParameter("model", "Gemini model id from the native URL path.", geminiEmbeddingExample),
				withSecurity(geminiSecurity),
			),
		},
		geminiStreamOperation(refs, "/v1beta/models/{model}:streamGenerateContent", "geminiStreamGenerateContentV1Beta", "Gemini streamGenerateContent (v1beta)", geminiChatExample, geminiSecurity),
		geminiStreamOperation(refs, "/v1/models/{model}:streamGenerateContent", "geminiStreamGenerateContentV1", "Gemini streamGenerateContent (v1)", geminiChatExample, geminiSecurity),
	}
}

func alternativeSecurity(base *openapi3.SecurityRequirements, scheme string) *openapi3.SecurityRequirements {
	if base == nil {
		return nil
	}
	out := openapi3.NewSecurityRequirements()
	for _, requirement := range *base {
		out.With(requirement)
	}
	out.With(openapi3.NewSecurityRequirement().Authenticate(scheme))
	return out
}

func geminiStreamOperation(refs schemaSet, path, operationID, summary, exampleModel string, security *openapi3.SecurityRequirements) pathOperation {
	return pathOperation{
		path: path,
		op: operation(
			operationID,
			summary,
			"Gemini-native SSE endpoint. The gateway preserves provider events while tracking final usage.",
			[]string{"provider"},
			requestBodyRef(geminiGenerateRequestSchemaRef(), map[string]any{
				"contents": []map[string]any{{
					"role":  "user",
					"parts": []map[string]any{{"text": "Count to three."}},
				}},
			}),
			responses(
				responseStatus("200", responseWithContent("Gemini-native SSE stream.", openapi3.Content{
					"text/event-stream": media(stringSchemaRef(), geminiSSEExample),
				})),
				responseStatus("default", geminiErrorResponse(refs)),
			),
			withPathParameter("model", "Gemini model id from the native URL path.", exampleModel),
			withSecurity(security),
		),
	}
}

func anthropicErrorResponse(refs schemaSet) *openapi3.ResponseRef {
	return jsonResponse(refs.ref(schemaAnthropicError), "Anthropic-native error response.", map[string]any{
		"type": "error",
		"error": map[string]any{
			"type":    "invalid_request_error",
			"message": "invalid request",
		},
	})
}

func openAIErrorResponse(refs schemaSet) *openapi3.ResponseRef {
	return jsonResponse(refs.ref(schemaErrorEnvelope), "OpenAI-compatible error response.", map[string]any{
		"error": map[string]any{
			"message": "invalid request",
			"type":    "invalid_request_error",
		},
	})
}

func geminiErrorResponse(refs schemaSet) *openapi3.ResponseRef {
	return jsonResponse(refs.ref(schemaGeminiError), "Google API error response.", map[string]any{
		"error": map[string]any{
			"code":    400,
			"message": "invalid request",
			"status":  "INVALID_ARGUMENT",
		},
	})
}

func responseStatus(status string, response *openapi3.ResponseRef) func(*openapi3.Responses) {
	return func(items *openapi3.Responses) {
		items.Set(status, response)
	}
}

func jsonResponse(schema *openapi3.SchemaRef, description string, example any) *openapi3.ResponseRef {
	return responseWithContent(description, openapi3.Content{
		"application/json": media(schema, example),
	})
}

func responseWithContent(description string, content openapi3.Content) *openapi3.ResponseRef {
	response := openapi3.NewResponse().WithDescription(description)
	if content != nil {
		response.WithContent(content)
	}
	return &openapi3.ResponseRef{Value: response}
}

func media(schema *openapi3.SchemaRef, example any) *openapi3.MediaType {
	out := openapi3.NewMediaType().WithSchemaRef(schema)
	if example != nil {
		out.Example = example
	}
	return out
}

func requestSchema(payload *openapi3.SchemaRef, models []string, withStream bool) *openapi3.SchemaRef {
	base := openapi3.NewObjectSchema().
		WithPropertyRef("model", modelSchema(models)).
		WithRequired([]string{"model"})
	if withStream {
		boolSchema := openapi3.NewBoolSchema()
		boolSchema.Default = false
		base.WithPropertyRef("stream", openapi3.NewSchemaRef("", boolSchema))
	}
	return openapi3.NewSchemaRef("", &openapi3.Schema{
		AllOf: openapi3.SchemaRefs{
			payload,
			openapi3.NewSchemaRef("", base),
		},
	})
}

func modelEnum(cfg *gateway.Snapshot) []string {
	if cfg == nil {
		return nil
	}
	return lo.Map(cfg.Models(), func(model gateway.ModelDescriptor, _ int) string { return model.ID })
}

func providerModelEnum(cfg *gateway.Snapshot, provider string, operations ...gateway.Operation) []string {
	if cfg == nil {
		return nil
	}
	seen := make(map[string]struct{})
	models := make([]string, 0)
	for _, route := range cfg.Routes {
		if route == nil || route.Provider != provider {
			continue
		}
		if !lo.SomeBy(operations, route.Capabilities.Supports) {
			continue
		}
		if _, ok := seen[route.Model]; ok {
			continue
		}
		seen[route.Model] = struct{}{}
		models = append(models, route.Model)
	}
	sort.Strings(models)
	return models
}

func firstModelOr(models []string, fallback string) string {
	if len(models) > 0 {
		return models[0]
	}
	return fallback
}

func modelSchema(enum []string) *openapi3.SchemaRef {
	schema := openapi3.NewStringSchema()
	schema.Description = "Configured public model alias. The selected route maps it to its upstream model."
	if len(enum) > 0 {
		schema.Enum = lo.Map(enum, func(value string, _ int) any { return value })
		schema.Example = enum[0]
	}
	return openapi3.NewSchemaRef("", schema)
}

func inferenceSecurity(cfg *gateway.Snapshot) *openapi3.SecurityRequirements {
	if cfg == nil || cfg.Auth.AllowAnonymous || (len(cfg.Auth.Tokens) == 0 && !cfg.Auth.JWT.Enabled()) {
		return nil
	}
	return openapi3.NewSecurityRequirements().With(openapi3.NewSecurityRequirement().Authenticate("bearerAuth"))
}

func schemaFor(schemas openapi3.Schemas, ref *openapi3.SchemaRef) *openapi3.Schema {
	if ref == nil {
		return nil
	}
	if ref.Value != nil {
		return ref.Value
	}
	name := strings.TrimPrefix(ref.Ref, "#/components/schemas/")
	if name == "" {
		return nil
	}
	if item, ok := schemas[name]; ok && item != nil {
		return item.Value
	}
	return nil
}

func inlineComponentSchemaRef(schemas openapi3.Schemas, ref *openapi3.SchemaRef) *openapi3.SchemaRef {
	schema := schemaFor(schemas, ref)
	if schema == nil {
		return ref
	}
	return openapi3.NewSchemaRef("", schema)
}

func setAnyAdditionalProperties(schema *openapi3.Schema) {
	if schema == nil {
		return
	}
	value := true
	schema.AdditionalProperties = openapi3.AdditionalProperties{Has: &value}
}

func setProperty(schema *openapi3.Schema, name string, ref *openapi3.SchemaRef) {
	if schema == nil {
		return
	}
	if schema.Properties == nil {
		schema.Properties = openapi3.Schemas{}
	}
	schema.Properties[name] = ref
}

func stringSchemaRef() *openapi3.SchemaRef {
	return openapi3.NewSchemaRef("", openapi3.NewStringSchema())
}

func integerSchemaRef() *openapi3.SchemaRef {
	return openapi3.NewSchemaRef("", openapi3.NewIntegerSchema())
}

func objectAnySchemaRef() *openapi3.SchemaRef {
	return openapi3.NewSchemaRef("", openapi3.NewObjectSchema().WithAnyAdditionalProperties())
}

func anthropicRequestSchemaRef() *openapi3.SchemaRef {
	schema := openapi3.NewObjectSchema().WithAnyAdditionalProperties().
		WithPropertyRef("model", stringSchemaRef()).
		WithPropertyRef("messages", arraySchemaRef(objectAnySchemaRef())).
		WithPropertyRef("max_tokens", integerSchemaRef()).
		WithPropertyRef("stream", openapi3.NewSchemaRef("", openapi3.NewBoolSchema())).
		WithRequired([]string{"model", "messages", "max_tokens"})
	return openapi3.NewSchemaRef("", schema)
}

func geminiGenerateRequestSchemaRef() *openapi3.SchemaRef {
	schema := openapi3.NewObjectSchema().WithAnyAdditionalProperties().
		WithPropertyRef("contents", arraySchemaRef(objectAnySchemaRef())).
		WithPropertyRef("tools", arraySchemaRef(objectAnySchemaRef())).
		WithPropertyRef("generationConfig", objectAnySchemaRef()).
		WithRequired([]string{"contents"})
	return openapi3.NewSchemaRef("", schema)
}

func geminiEmbeddingRequestSchemaRef() *openapi3.SchemaRef {
	schema := openapi3.NewObjectSchema().WithAnyAdditionalProperties().
		WithPropertyRef("content", objectAnySchemaRef()).
		WithRequired([]string{"content"})
	return openapi3.NewSchemaRef("", schema)
}

func nullableObjectAnySchemaRef() *openapi3.SchemaRef {
	schema := openapi3.NewObjectSchema().WithAnyAdditionalProperties()
	schema.Nullable = true
	return openapi3.NewSchemaRef("", schema)
}

func nullableStringSchemaRef() *openapi3.SchemaRef {
	schema := openapi3.NewStringSchema()
	schema.Nullable = true
	return openapi3.NewSchemaRef("", schema)
}

func stringOrObjectSchemaRef() *openapi3.SchemaRef {
	return openapi3.NewSchemaRef("", &openapi3.Schema{
		OneOf: openapi3.SchemaRefs{
			stringSchemaRef(),
			objectAnySchemaRef(),
		},
	})
}

func arraySchemaRef(items *openapi3.SchemaRef) *openapi3.SchemaRef {
	types := openapi3.Types{"array"}
	return openapi3.NewSchemaRef("", &openapi3.Schema{
		Type:  &types,
		Items: items,
	})
}

func (s schemaSet) ref(key string) *openapi3.SchemaRef {
	if s == nil {
		return nil
	}
	return s[key]
}

func contentUnionSchemaRef(contentPart *openapi3.SchemaRef) *openapi3.SchemaRef {
	return openapi3.NewSchemaRef("", &openapi3.Schema{
		OneOf: openapi3.SchemaRefs{
			stringSchemaRef(),
			contentPart,
			arraySchemaRef(contentPart),
		},
	})
}

func responsesInputSchemaRef(message, contentPart *openapi3.SchemaRef) *openapi3.SchemaRef {
	return openapi3.NewSchemaRef("", &openapi3.Schema{
		Description: "Accepts a string, a single message, a content-part array, or a full message array.",
		OneOf: openapi3.SchemaRefs{
			stringSchemaRef(),
			message,
			arraySchemaRef(contentPart),
			arraySchemaRef(message),
		},
	})
}

func textInputSchemaRef() *openapi3.SchemaRef {
	return openapi3.NewSchemaRef("", &openapi3.Schema{
		Description: "Accepts a single string, an array of strings, a token array, or a matrix of token arrays.",
		OneOf: openapi3.SchemaRefs{
			stringSchemaRef(),
			arraySchemaRef(stringSchemaRef()),
			arraySchemaRef(integerSchemaRef()),
			arraySchemaRef(arraySchemaRef(integerSchemaRef())),
		},
	})
}

func inlineGeneratedComponentRefs(schemas openapi3.Schemas) {
	seen := map[*openapi3.SchemaRef]bool{}
	for _, ref := range schemas {
		inlineSchemaRef(ref, schemas, seen)
	}
}

func inlineSchemaRef(ref *openapi3.SchemaRef, schemas openapi3.Schemas, seen map[*openapi3.SchemaRef]bool) {
	if ref == nil || seen[ref] {
		return
	}
	seen[ref] = true
	defer delete(seen, ref)

	if ref.Ref != "" && ref.Value == nil {
		name := strings.TrimPrefix(ref.Ref, "#/components/schemas/")
		if name != ref.Ref {
			if resolved := schemas[name]; resolved != nil && resolved.Value != nil {
				ref.Ref = ""
				ref.Value = resolved.Value
			}
		}
	}
	if ref.Value == nil {
		return
	}
	inlineSchema(ref.Value, schemas, seen)
}

func inlineSchema(schema *openapi3.Schema, schemas openapi3.Schemas, seen map[*openapi3.SchemaRef]bool) {
	if schema == nil {
		return
	}
	for _, ref := range schema.OneOf {
		inlineSchemaRef(ref, schemas, seen)
	}
	for _, ref := range schema.AnyOf {
		inlineSchemaRef(ref, schemas, seen)
	}
	for _, ref := range schema.AllOf {
		inlineSchemaRef(ref, schemas, seen)
	}
	inlineSchemaRef(schema.Not, schemas, seen)
	inlineSchemaRef(schema.Items, schemas, seen)
	for _, ref := range schema.Properties {
		inlineSchemaRef(ref, schemas, seen)
	}
	inlineSchemaRef(schema.AdditionalProperties.Schema, schemas, seen)
}
