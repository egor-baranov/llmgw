package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"

	"llmgw/gateway"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3gen"
	"github.com/samber/lo"
	"gopkg.in/yaml.v3"
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

type modelListResponseDoc struct {
	Object string                    `json:"object"`
	Data   []gateway.ModelDescriptor `json:"data"`
}

type limitsResponseDoc struct {
	KeyID  string              `json:"key_id"`
	Source string              `json:"source"`
	Limits gateway.LimitSpec   `json:"limits"`
	Usage  *gateway.QuotaUsage `json:"usage,omitempty"`
}

type inputAudioDoc struct {
	Data   string `json:"data,omitempty"`
	Format string `json:"format,omitempty"`
}

type contentPartDoc struct {
	Type       string         `json:"type,omitempty"`
	Text       string         `json:"text,omitempty"`
	Refusal    string         `json:"refusal,omitempty"`
	ImageURL   string         `json:"image_url,omitempty"`
	InputText  string         `json:"input_text,omitempty"`
	InputAudio *inputAudioDoc `json:"input_audio,omitempty"`
	FileID     string         `json:"file_id,omitempty"`
	MIMEType   string         `json:"mime_type,omitempty"`
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
}

type responseOutputDoc struct {
	Type    string           `json:"type,omitempty"`
	ID      string           `json:"id,omitempty"`
	Role    string           `json:"role,omitempty"`
	Content []contentPartDoc `json:"content,omitempty"`
}

type embeddingDataDoc struct {
	Object    string    `json:"object,omitempty"`
	Index     int       `json:"index,omitempty"`
	Embedding []float64 `json:"embedding,omitempty"`
}

type chatRequestDoc struct {
	Messages        []messageDoc       `json:"messages,omitempty"`
	Tools           []toolDoc          `json:"tools,omitempty"`
	ToolChoice      json.RawMessage    `json:"tool_choice,omitempty"`
	ResponseFormat  *responseFormatDoc `json:"response_format,omitempty"`
	Metadata        map[string]string  `json:"metadata,omitempty"`
	Reasoning       *reasoningDoc      `json:"reasoning,omitempty"`
	MaxTokens       int                `json:"max_tokens,omitempty"`
	MaxOutputTokens int                `json:"max_output_tokens,omitempty"`
	User            string             `json:"user,omitempty"`
}

type responsesRequestDoc struct {
	Input           json.RawMessage    `json:"input,omitempty"`
	Instructions    string             `json:"instructions,omitempty"`
	Tools           []toolDoc          `json:"tools,omitempty"`
	ToolChoice      json.RawMessage    `json:"tool_choice,omitempty"`
	ResponseFormat  *responseFormatDoc `json:"response_format,omitempty"`
	Metadata        map[string]string  `json:"metadata,omitempty"`
	Reasoning       *reasoningDoc      `json:"reasoning,omitempty"`
	MaxOutputTokens int                `json:"max_output_tokens,omitempty"`
	User            string             `json:"user,omitempty"`
}

type completionRequestDoc struct {
	Prompt    json.RawMessage   `json:"prompt,omitempty"`
	Suffix    string            `json:"suffix,omitempty"`
	MaxTokens int               `json:"max_tokens,omitempty"`
	User      string            `json:"user,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

type embeddingRequestDoc struct {
	Input          json.RawMessage   `json:"input,omitempty"`
	EncodingFormat string            `json:"encoding_format,omitempty"`
	Dimensions     int               `json:"dimensions,omitempty"`
	User           string            `json:"user,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
}

type chatChoiceDoc struct {
	Index        int        `json:"index,omitempty"`
	Message      messageDoc `json:"message,omitempty"`
	FinishReason string     `json:"finish_reason,omitempty"`
}

type chatResponseDoc struct {
	ID      string          `json:"id,omitempty"`
	Object  string          `json:"object,omitempty"`
	Model   string          `json:"model,omitempty"`
	Created int64           `json:"created,omitempty"`
	Choices []chatChoiceDoc `json:"choices,omitempty"`
	Usage   gateway.Usage   `json:"usage,omitempty"`
}

type responsesResponseDoc struct {
	ID         string              `json:"id,omitempty"`
	Object     string              `json:"object,omitempty"`
	Model      string              `json:"model,omitempty"`
	Created    int64               `json:"created,omitempty"`
	Status     string              `json:"status,omitempty"`
	Output     []responseOutputDoc `json:"output,omitempty"`
	OutputText string              `json:"output_text,omitempty"`
	Usage      gateway.Usage       `json:"usage,omitempty"`
}

type completionChoiceDoc struct {
	Index        int    `json:"index,omitempty"`
	Text         string `json:"text,omitempty"`
	FinishReason string `json:"finish_reason,omitempty"`
}

type completionResponseDoc struct {
	ID      string                `json:"id,omitempty"`
	Object  string                `json:"object,omitempty"`
	Model   string                `json:"model,omitempty"`
	Created int64                 `json:"created,omitempty"`
	Choices []completionChoiceDoc `json:"choices,omitempty"`
	Usage   gateway.Usage         `json:"usage,omitempty"`
}

type embeddingsResponseDoc struct {
	Object string             `json:"object,omitempty"`
	Model  string             `json:"model,omitempty"`
	Data   []embeddingDataDoc `json:"data,omitempty"`
	Usage  gateway.Usage      `json:"usage,omitempty"`
}

type schemaSet map[string]*openapi3.SchemaRef

const (
	schemaHealthz       = "Healthz"
	schemaReadyz        = "Readyz"
	schemaErrorEnvelope = "ErrorEnvelope"
	schemaModel         = "Model"
	schemaModelList     = "ModelList"
	schemaInputAudio    = "InputAudio"
	schemaContentPart   = "ContentPart"
	schemaTool          = "Tool"
	schemaToolCall      = "ToolCall"
	schemaJSONSchema    = "JSONSchema"
	schemaResponseFmt   = "ResponseFormat"
	schemaReasoning     = "Reasoning"
	schemaMessage       = "Message"
	schemaUsage         = "Usage"
	schemaDuration      = "Duration"
	schemaQuotaUsage    = "QuotaUsage"
	schemaLimitSpec     = "LimitSpec"
	schemaQuotaLimits   = "QuotaLimitsResponse"
	schemaEmbeddingData = "EmbeddingData"
	schemaResponseOut   = "ResponseOutput"
	schemaChatReq       = "ChatRequest"
	schemaResponsesReq  = "ResponsesRequest"
	schemaCompletionReq = "CompletionRequest"
	schemaEmbeddingReq  = "EmbeddingRequest"
	schemaChatResp      = "ChatResponse"
	schemaResponsesResp = "ResponsesResponse"
	schemaCompletionRes = "CompletionResponse"
	schemaEmbeddingRes  = "EmbeddingsResponse"
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
			&openapi3.Server{URL: serverURL(cfg), Description: "Configured gateway listener"},
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
	))})
	doc.Paths.Set("/openapi.yaml", &openapi3.PathItem{Get: operation("openapiYAML", "Generated OpenAPI YAML", "", []string{"docs"}, nil, responses(
		responseStatus("200", responseWithContent("Live OpenAPI document for the gateway surface.", openapi3.Content{
			"application/yaml": media(stringSchemaRef(), nil),
			"text/yaml":        media(stringSchemaRef(), nil),
		})),
	))})
	doc.Paths.Set("/openapi.json", &openapi3.PathItem{Get: operation("openapiJSON", "Generated OpenAPI JSON", "", []string{"docs"}, nil, responses(
		responseStatus("200", responseWithContent("Live OpenAPI document for the gateway surface.", openapi3.Content{
			"application/json": media(stringSchemaRef(), nil),
		})),
	))})
	doc.Paths.Set("/docs", &openapi3.PathItem{Get: operation("docsRedirect", "Redirect to Swagger UI", "", []string{"docs"}, nil, responses(
		responseStatus("301", responseWithContent("Redirects to /docs/index.html.", nil)),
	))})
	doc.Paths.Set("/docs/index.html", &openapi3.PathItem{Get: operation("docs", "Swagger UI", "", []string{"docs"}, nil, responses(
		responseStatus("200", responseWithContent("Swagger UI backed by /openapi.json.", openapi3.Content{
			"text/html": media(stringSchemaRef(), nil),
		})),
	))})
	doc.Paths.Set("/v1/models", &openapi3.PathItem{Get: operation("listModels", "List configured route models", "", []string{"system"}, nil, responses(
		responseStatus("200", jsonResponse(refs.ref(schemaModelList), "Configured public models.", map[string]any{
			"object": "list",
			"data":   []map[string]any{{"id": exampleModel, "object": "model", "owned_by": "llmgw"}},
		})),
		responseStatus("default", jsonResponse(refs.ref(schemaErrorEnvelope), "Error response.", map[string]any{"error": map[string]any{"message": "request failed", "type": "invalid_request_error"}})),
	))})
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
		Put: operation("putLimits", "Set quota limits for the authenticated token key", "", []string{"quota"}, requestBodyRef(refs.ref(schemaLimitSpec), map[string]any{
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
	for _, item := range providerOperations(refs, exampleModel, security) {
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

	setProperty(schemaFor(schemas, refs.ref(schemaTool)), "parameters", objectAnySchemaRef())
	setProperty(schemaFor(schemas, refs.ref(schemaTool)), "function", objectAnySchemaRef())
	setProperty(schemaFor(schemas, refs.ref(schemaToolCall)), "function", objectAnySchemaRef())
	setProperty(schemaFor(schemas, refs.ref(schemaJSONSchema)), "schema", objectAnySchemaRef())
	setProperty(schemaFor(schemas, refs.ref(schemaChatReq)), "tool_choice", stringOrObjectSchemaRef())
	setProperty(schemaFor(schemas, refs.ref(schemaResponsesReq)), "tool_choice", stringOrObjectSchemaRef())
	setProperty(schemaFor(schemas, refs.ref(schemaLimitSpec)), "budget_duration", stringSchemaRef())
	setProperty(schemaFor(schemas, refs.ref(schemaMessage)), "content", contentUnionSchemaRef(refs.ref(schemaContentPart)))
	setProperty(schemaFor(schemas, refs.ref(schemaResponsesReq)), "input", responsesInputSchemaRef(refs.ref(schemaMessage), refs.ref(schemaContentPart)))
	setProperty(schemaFor(schemas, refs.ref(schemaCompletionReq)), "prompt", textInputSchemaRef())
	setProperty(schemaFor(schemas, refs.ref(schemaEmbeddingReq)), "input", textInputSchemaRef())
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

func providerOperations(refs schemaSet, exampleModel string, security *openapi3.SecurityRequirements) []pathOperation {
	return []pathOperation{
		{
			path: "/v1/chat/completions",
			op: operation(
				"openAIChatCompletions",
				"OpenAI chat completions",
				"OpenAI-native endpoint. Requests are routed only to OpenAI routes and are not translated across provider families.",
				[]string{"provider"},
				requestBodyRef(requestSchema(refs.ref(schemaChatReq), []string{exampleModel}, true), map[string]any{
					"model": exampleModel,
					"messages": []map[string]any{{
						"role":    "user",
						"content": "Say hello in one sentence.",
					}},
				}),
				responses(
					responseStatus("200", responseWithContent("OpenAI-native chat completion or SSE stream.", openapi3.Content{
						"application/json":  media(refs.ref(schemaChatResp), nil),
						"text/event-stream": media(stringSchemaRef(), sseExample("")),
					})),
					responseStatus("default", jsonResponse(refs.ref(schemaErrorEnvelope), "Error response.", map[string]any{"error": map[string]any{"message": "invalid request", "type": "invalid_request_error"}})),
				),
				withSecurity(security),
			),
		},
		{
			path: "/v1/responses",
			op: operation(
				"openAIResponses",
				"OpenAI responses",
				"OpenAI-native endpoint. Requests are routed only to OpenAI routes and are not translated across provider families.",
				[]string{"provider"},
				requestBodyRef(requestSchema(refs.ref(schemaResponsesReq), []string{exampleModel}, true), map[string]any{
					"model": exampleModel,
					"input": "Summarize the gateway in one line.",
				}),
				responses(
					responseStatus("200", responseWithContent("OpenAI-native response JSON or SSE stream.", openapi3.Content{
						"application/json":  media(refs.ref(schemaResponsesResp), nil),
						"text/event-stream": media(stringSchemaRef(), sseExample("response.created")),
					})),
					responseStatus("default", jsonResponse(refs.ref(schemaErrorEnvelope), "Error response.", map[string]any{"error": map[string]any{"message": "invalid request", "type": "invalid_request_error"}})),
				),
				withSecurity(security),
			),
		},
		{
			path: "/v1/completions",
			op: operation(
				"openAICompletions",
				"OpenAI legacy completions",
				"OpenAI-native endpoint. Requests are routed only to OpenAI routes and are not translated across provider families.",
				[]string{"provider"},
				requestBodyRef(requestSchema(refs.ref(schemaCompletionReq), []string{exampleModel}, true), map[string]any{
					"model":  exampleModel,
					"prompt": "Write one short sentence about Go.",
				}),
				responses(
					responseStatus("200", responseWithContent("OpenAI-native completion JSON or SSE stream.", openapi3.Content{
						"application/json":  media(refs.ref(schemaCompletionRes), nil),
						"text/event-stream": media(stringSchemaRef(), sseExample("")),
					})),
					responseStatus("default", jsonResponse(refs.ref(schemaErrorEnvelope), "Error response.", map[string]any{"error": map[string]any{"message": "invalid request", "type": "invalid_request_error"}})),
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
				requestBodyRef(requestSchema(refs.ref(schemaEmbeddingReq), []string{exampleModel}, false), map[string]any{
					"model": exampleModel,
					"input": "gateway",
				}),
				responses(
					responseStatus("200", jsonResponse(refs.ref(schemaEmbeddingRes), "OpenAI-native embeddings response.", map[string]any{
						"object": "list",
						"model":  exampleModel,
						"data":   []map[string]any{{"object": "embedding", "index": 0, "embedding": []float64{0.1, 0.2}}},
						"usage":  map[string]any{"prompt_tokens": 1, "total_tokens": 1},
					})),
					responseStatus("default", jsonResponse(refs.ref(schemaErrorEnvelope), "Error response.", map[string]any{"error": map[string]any{"message": "invalid request", "type": "invalid_request_error"}})),
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
				requestBodyRef(objectAnySchemaRef(), map[string]any{
					"model": exampleModel,
					"messages": []map[string]any{{
						"role":    "user",
						"content": "Say hello in one sentence.",
					}},
				}),
				responses(
					responseStatus("200", jsonResponse(objectAnySchemaRef(), "Anthropic-native message response.", map[string]any{
						"id":    "msg_1",
						"type":  "message",
						"role":  "assistant",
						"model": exampleModel,
					})),
					responseStatus("default", jsonResponse(refs.ref(schemaErrorEnvelope), "Error response.", map[string]any{"error": map[string]any{"message": "invalid request", "type": "invalid_request_error"}})),
				),
				withSecurity(security),
			),
		},
		{
			path: "/v1beta/models/{model}:generateContent",
			op: operation(
				"geminiGenerateContentV1Beta",
				"Gemini generateContent (v1beta)",
				"Gemini-native endpoint. Requests are routed only to Gemini routes and are not translated across provider families.",
				[]string{"provider"},
				requestBodyRef(objectAnySchemaRef(), map[string]any{
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
					responseStatus("default", jsonResponse(refs.ref(schemaErrorEnvelope), "Error response.", map[string]any{"error": map[string]any{"message": "invalid request", "type": "invalid_request_error"}})),
				),
				withPathParameter("model", "Gemini model id from the native URL path.", exampleModel),
				withSecurity(security),
			),
		},
		{
			path: "/v1beta/models/{model}:embedContent",
			op: operation(
				"geminiEmbedContentV1Beta",
				"Gemini embedContent (v1beta)",
				"Gemini-native endpoint. Requests are routed only to Gemini routes and are not translated across provider families.",
				[]string{"provider"},
				requestBodyRef(objectAnySchemaRef(), map[string]any{
					"content": map[string]any{
						"parts": []map[string]any{{"text": "gateway"}},
					},
				}),
				responses(
					responseStatus("200", jsonResponse(objectAnySchemaRef(), "Gemini-native embedContent response.", map[string]any{
						"embeddings": []map[string]any{{"values": []float64{0.1, 0.2}}},
					})),
					responseStatus("default", jsonResponse(refs.ref(schemaErrorEnvelope), "Error response.", map[string]any{"error": map[string]any{"message": "invalid request", "type": "invalid_request_error"}})),
				),
				withPathParameter("model", "Gemini model id from the native URL path.", exampleModel),
				withSecurity(security),
			),
		},
		{
			path: "/v1/models/{model}:generateContent",
			op: operation(
				"geminiGenerateContentV1",
				"Gemini generateContent (v1)",
				"Gemini-native endpoint. Requests are routed only to Gemini routes and are not translated across provider families.",
				[]string{"provider"},
				requestBodyRef(objectAnySchemaRef(), map[string]any{
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
					responseStatus("default", jsonResponse(refs.ref(schemaErrorEnvelope), "Error response.", map[string]any{"error": map[string]any{"message": "invalid request", "type": "invalid_request_error"}})),
				),
				withPathParameter("model", "Gemini model id from the native URL path.", exampleModel),
				withSecurity(security),
			),
		},
		{
			path: "/v1/models/{model}:embedContent",
			op: operation(
				"geminiEmbedContentV1",
				"Gemini embedContent (v1)",
				"Gemini-native endpoint. Requests are routed only to Gemini routes and are not translated across provider families.",
				[]string{"provider"},
				requestBodyRef(objectAnySchemaRef(), map[string]any{
					"content": map[string]any{
						"parts": []map[string]any{{"text": "gateway"}},
					},
				}),
				responses(
					responseStatus("200", jsonResponse(objectAnySchemaRef(), "Gemini-native embedContent response.", map[string]any{
						"embeddings": []map[string]any{{"values": []float64{0.1, 0.2}}},
					})),
					responseStatus("default", jsonResponse(refs.ref(schemaErrorEnvelope), "Error response.", map[string]any{"error": map[string]any{"message": "invalid request", "type": "invalid_request_error"}})),
				),
				withPathParameter("model", "Gemini model id from the native URL path.", exampleModel),
				withSecurity(security),
			),
		},
	}
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

func modelSchema(enum []string) *openapi3.SchemaRef {
	schema := openapi3.NewStringSchema()
	schema.Description = "Configured route model name. Requests are matched directly against route models."
	if len(enum) > 0 {
		schema.Enum = lo.Map(enum, func(value string, _ int) any { return value })
		schema.Example = enum[0]
	}
	return openapi3.NewSchemaRef("", schema)
}

func inferenceSecurity(cfg *gateway.Snapshot) *openapi3.SecurityRequirements {
	if cfg == nil || (len(cfg.Auth.Tokens) == 0 && !cfg.Auth.JWT.Enabled()) {
		return nil
	}
	return openapi3.NewSecurityRequirements().With(openapi3.NewSecurityRequirement().Authenticate("bearerAuth"))
}

func serverURL(cfg *gateway.Snapshot) string {
	listen := ":8080"
	if cfg != nil && cfg.Server.Listen != "" {
		listen = cfg.Server.Listen
	}
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		if strings.HasPrefix(listen, ":") {
			return "http://localhost" + listen
		}
		return "http://" + strings.TrimSpace(listen)
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "localhost"
	}
	return fmt.Sprintf("http://%s:%s", host, port)
}

func sseExample(event string) string {
	payload := "data: {\"id\":\"evt_1\"}\n\n" +
		"data: [DONE]\n\n"
	if event == "" {
		return payload
	}
	return "event: " + event + "\n" + payload
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
