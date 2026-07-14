package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"mime"
	"net/http"
	"strings"

	"llmgw/gateway"
)

type Provider struct {
	adapter Adapter
	client  *http.Client
}

const (
	maxUnaryResponseBytes     = int64(16 << 20)
	maxEmbeddingResponseBytes = int64(64 << 20)
	maxUpstreamErrorBytes     = int64(1 << 20)
)

var legacyAdapterFactories = map[string]func() Adapter{
	"anthropic": AnthropicAdapter,
	"gemini":    GeminiAdapter,
	"openai":    OpenAIAdapter,
}

// New preserves the original name-based constructor for package consumers.
// Deprecated: use NewProvider with an explicit adapter.
func New(name string, client *http.Client) *Provider {
	if factory := legacyAdapterFactories[name]; factory != nil {
		return NewProvider(factory(), client)
	}
	return NewProvider(Adapter{Name: name}, client)
}

// NewProvider constructs the generic transport runtime around an explicit
// adapter. The gateway.Provider interface remains unchanged.
func NewProvider(adapter Adapter, client *http.Client) *Provider {
	if client == nil {
		client = http.DefaultClient
	}
	// API calls are expected to target their final endpoint. Refusing redirects
	// prevents provider credentials in non-standard headers from being copied to
	// an unrelated host by net/http's redirect machinery.
	safeClient := *client
	safeClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &Provider{adapter: cloneAdapter(adapter), client: &safeClient}
}

func (p *Provider) Name() string { return p.adapter.Name }

func (p *Provider) Supports(op gateway.Operation) bool {
	_, ok := p.adapter.operation(op)
	return ok
}

// ValidateRoute lets config assembly delegate provider-specific route semantics
// without expanding gateway.Provider's hot-path interface.
func (p *Provider) ValidateRoute(route *gateway.Route) error {
	if route == nil {
		return gateway.UnsupportedOperation("missing route")
	}
	if p.adapter.Name != "" && route.Provider != "" && route.Provider != p.adapter.Name {
		return fmt.Errorf("route %s provider %q does not match adapter %q", route.Name, route.Provider, p.adapter.Name)
	}
	if p.adapter.ValidateRoute != nil {
		return p.adapter.ValidateRoute(route)
	}
	return nil
}

// PlanBridge delegates compatibility routing to the provider protocol adapter.
// Routers call this only when the route lacks native support for the request.
func (p *Provider) PlanBridge(route *gateway.Route, req *gateway.Request) (gateway.Operation, string, bool) {
	if p.adapter.PlanBridge == nil {
		return "", "route does not support requested operation", false
	}
	return p.adapter.PlanBridge(route, req)
}

// ProjectTokenText removes provider-specific inline binary payloads before a
// tokenizer sees the request. Unknown adapters conservatively retain raw JSON.
func (p *Provider) ProjectTokenText(raw json.RawMessage) string {
	if p.adapter.ProjectTokenText == nil {
		return string(raw)
	}
	return p.adapter.ProjectTokenText(raw)
}

func (p *Provider) BuildEffective(_ gateway.ResolvedRoute, req *gateway.Request) (*gateway.EffectiveParams, error) {
	effective := &gateway.EffectiveParams{}
	if req != nil {
		effective.MaxOutputTokens = req.RequestedMaxOutputTokens()
	}
	return effective, nil
}

func (p *Provider) Preflight(route gateway.ResolvedRoute, req *gateway.Request) error {
	if p.adapter.Preflight == nil {
		return nil
	}
	return p.adapter.Preflight(route, req)
}

func (p *Provider) Invoke(ctx context.Context, route gateway.ResolvedRoute, req *gateway.Request) (*gateway.Result, error) {
	if req == nil {
		return nil, gateway.WithoutAttemptCharge(gateway.NewError(http.StatusBadRequest, "invalid_request_error", "invalid_request", "request is required"))
	}
	operation, ok := p.adapter.operation(req.Operation)
	if !ok {
		return nil, gateway.WithoutAttemptCharge(gateway.UnsupportedOperation("provider adapter does not support requested operation"))
	}
	prepared, err := operation.Prepare(route, req)
	if err != nil {
		return nil, gateway.WithoutAttemptCharge(err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, prepared.URL, bytes.NewReader(prepared.Body))
	if err != nil {
		return nil, gateway.WithoutAttemptCharge(gateway.NewError(http.StatusBadGateway, "upstream_error", "request_failed", "could not build upstream request"))
	}
	httpReq.Header.Set("Content-Type", "application/json")
	applyRequestHeaders(httpReq, route, req.Meta, p.adapter.ForwardHeaders)
	if p.adapter.ApplyAuth != nil {
		if err := p.adapter.ApplyAuth(httpReq, route.Route); err != nil {
			return nil, gateway.WithoutAttemptCharge(err)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, gateway.WithoutAttemptCharge(err)
	}
	resp, err := p.client.Do(httpReq)
	if err != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			return nil, context.Canceled
		}
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, gateway.NewError(http.StatusGatewayTimeout, "upstream_error", "upstream_timeout", "upstream request timed out").
				WithCause(err).
				WithDisposition(true, true, true)
		}
		return nil, gateway.NewError(http.StatusBadGateway, "upstream_error", "request_failed", "upstream request failed").
			WithCause(err).
			WithDisposition(true, true, true)
	}
	limit := responseBodyLimit(route, req)
	if req.Stream {
		if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
			defer resp.Body.Close()
			body, tooLarge, readErr := readBounded(resp.Body, maxUpstreamErrorBytes)
			if readErr != nil {
				return nil, gateway.NewError(http.StatusBadGateway, "upstream_error", "read_failed", "failed to read upstream response").
					WithCause(readErr).
					WithDisposition(true, true, true)
			}
			if tooLarge {
				return nil, responseTooLargeError()
			}
			return nil, upstreamAttemptError(resp.StatusCode, p.parseError(resp.StatusCode, body), p.extractUsage(route, req, body), safeUpstreamErrorHeaders(resp.Header))
		}
		mediaType, _, mediaErr := mime.ParseMediaType(resp.Header.Get("Content-Type"))
		if mediaErr != nil || !strings.EqualFold(mediaType, "text/event-stream") {
			_ = resp.Body.Close()
			return nil, invalidUpstreamResponse("stream response is not SSE")
		}
		tracker := newStreamUsageTracker(p.adapter.Stream, route, req)
		responseHeaders := cloneHeaders(resp.Header)
		stripEchoedRequestHeaders(responseHeaders, httpReq.Header)
		stream := tracker.Wrap(newResponseLimitReadCloser(resp.Body, limit))
		transformed := false
		if p.adapter.Stream.Transform != nil {
			current := stream
			stream, transformed, err = p.adapter.Stream.Transform(route, req, current)
			if err != nil {
				_ = current.Close()
				return nil, err
			}
			if stream == nil {
				_ = current.Close()
				return nil, gateway.NewError(http.StatusInternalServerError, "server_error", "invalid_adapter", "provider stream adapter returned no stream")
			}
		}
		if transformed {
			stream = newResponseLimitReadCloser(stream, limit)
			stripTransformedResponseHeaders(responseHeaders)
		}
		return &gateway.Result{
			StatusCode:    resp.StatusCode,
			Headers:       responseHeaders,
			ContentType:   firstNonEmpty(resp.Header.Get("Content-Type"), "text/event-stream"),
			RawStream:     stream,
			Usage:         fallbackUsage(req, route.Estimate),
			UsageSnapshot: tracker.Usage,
		}, nil
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		limit = maxUpstreamErrorBytes
	}
	data, tooLarge, err := readBounded(resp.Body, limit)
	if err != nil {
		return nil, gateway.NewError(http.StatusBadGateway, "upstream_error", "read_failed", "failed to read upstream response").
			WithCause(err).
			WithDisposition(true, true, true)
	}
	if tooLarge {
		return nil, responseTooLargeError()
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, upstreamAttemptError(resp.StatusCode, p.parseError(resp.StatusCode, data), p.extractUsage(route, req, data), safeUpstreamErrorHeaders(resp.Header))
	}
	if operation.ValidateResponse != nil {
		if err := operation.ValidateResponse(data); err != nil {
			return nil, err
		}
	}
	usage := p.extractUsage(route, req, data)
	responseHeaders := cloneHeaders(resp.Header)
	stripEchoedRequestHeaders(responseHeaders, httpReq.Header)
	contentType := firstNonEmpty(resp.Header.Get("Content-Type"), "application/json")
	transformed := false
	if p.adapter.TransformResponse != nil {
		data, transformed, err = p.adapter.TransformResponse(route, req, data)
		if err != nil {
			return nil, gateway.WithAttemptUsage(gateway.AllowFallback(err), usage)
		}
	}
	if transformed {
		if int64(len(data)) > limit {
			return nil, responseTooLargeError()
		}
		stripTransformedResponseHeaders(responseHeaders)
		contentType = "application/json"
	}
	usage = gateway.ReconcileReportedUsage(usage, fallbackUsage(req, route.Estimate))
	return &gateway.Result{
		StatusCode:  resp.StatusCode,
		Headers:     responseHeaders,
		ContentType: contentType,
		RawBody:     data,
		Usage:       usage,
	}, nil
}

func (p *Provider) parseError(status int, body []byte) error {
	if p.adapter.ParseError != nil {
		if err := p.adapter.ParseError(status, body); err != nil {
			return err
		}
	}
	return genericUpstreamError(status)
}

func (p *Provider) extractUsage(route gateway.ResolvedRoute, req *gateway.Request, body []byte) gateway.Usage {
	if p.adapter.ExtractUsage == nil {
		return gateway.Usage{}
	}
	return p.adapter.ExtractUsage(route, req, body)
}

/*
	Provider-specific request preparation, validation, error decoding, usage
	extraction, and stream terminal semantics live in adapter_*.go. Keep the
	remaining helpers below protocol-neutral so adding an adapter cannot require a
	core switch.
*/

func invalidUpstreamResponse(detail string) error {
	return gateway.NewError(http.StatusBadGateway, "upstream_error", "invalid_upstream_response", "upstream returned an invalid response: "+detail).
		WithDisposition(false, true, true)
}

func responseBodyLimit(route gateway.ResolvedRoute, req *gateway.Request) int64 {
	limit := maxUnaryResponseBytes
	if req != nil && req.Operation == gateway.OpEmbeddings {
		limit = maxEmbeddingResponseBytes
	}
	if route.Route != nil && route.Route.Limits.MaxResponseBytes > 0 {
		limit = route.Route.Limits.MaxResponseBytes
	}
	return limit
}

func responseTooLargeError() error {
	return gateway.NewError(http.StatusBadGateway, "upstream_error", "response_too_large", "upstream response exceeds the gateway limit").
		WithDisposition(false, true, false)
}

func upstreamAttemptError(status int, err error, usage gateway.Usage, headers http.Header) error {
	err = gateway.WithResponseHeaders(err, headers)
	// Providers do not bill ordinary rejected 4xx calls when they report no
	// usage. In particular, charging a full completion estimate for a 429 would
	// make a healthy fallback look as expensive as two generated responses.
	if status < http.StatusInternalServerError && usage.IsZero() {
		return gateway.WithoutAttemptCharge(err)
	}
	return gateway.WithAttemptUsage(err, usage)
}

func safeUpstreamErrorHeaders(headers http.Header) http.Header {
	out := make(http.Header)
	for key, values := range headers {
		lower := strings.ToLower(key)
		if !strings.EqualFold(key, "Retry-After") &&
			!strings.EqualFold(key, "X-Request-ID") &&
			!strings.EqualFold(key, "Request-ID") &&
			!strings.HasPrefix(lower, "x-ratelimit-") &&
			!strings.HasPrefix(lower, "ratelimit-") &&
			!strings.HasPrefix(lower, "anthropic-ratelimit-") {
			continue
		}
		for _, value := range values {
			out.Add(key, value)
		}
	}
	return out
}

func stripTransformedResponseHeaders(headers http.Header) {
	for _, key := range []string{
		"Content-Length",
		"Content-Encoding",
		"Content-Range",
		"Content-MD5",
		"Digest",
		"Content-Digest",
		"Repr-Digest",
		"ETag",
	} {
		headers.Del(key)
	}
}

// stripEchoedRequestHeaders removes same-name request metadata before response
// headers leave the proxy boundary. This protects arbitrary route-configured
// credentials whose names are not known to the API denylist. A small set of
// headers with legitimate request and response semantics is retained.
func stripEchoedRequestHeaders(response, request http.Header) {
	for key := range request {
		if responseHeaderMayOverlapRequest(key) {
			continue
		}
		response.Del(key)
	}
}

func responseHeaderMayOverlapRequest(key string) bool {
	lower := strings.ToLower(strings.TrimSpace(key))
	switch lower {
	case "accept-ranges", "age", "allow", "alt-svc", "cache-control",
		"content-digest", "content-disposition", "content-encoding", "content-language", "content-length",
		"content-location", "content-range", "content-type", "date", "digest", "etag", "expires",
		"last-modified", "link", "location", "request-id", "repr-digest", "retry-after", "server",
		"timing-allow-origin", "vary", "warning", "www-authenticate", "x-correlation-id", "x-request-id":
		return true
	}
	return strings.HasPrefix(lower, "access-control-") ||
		strings.HasPrefix(lower, "x-ratelimit-") ||
		strings.HasPrefix(lower, "ratelimit-") ||
		strings.HasPrefix(lower, "anthropic-ratelimit-")
}

func applyRequestHeaders(req *http.Request, resolved gateway.ResolvedRoute, meta gateway.Meta, forward func(http.Header, http.Header)) {
	if meta.RequestID != "" {
		req.Header.Set("X-Request-ID", meta.RequestID)
	}
	if forward != nil {
		forward(req.Header, meta.Headers)
	}
	for key, value := range resolved.Route.Headers {
		req.Header.Set(key, value)
	}
	for key, values := range resolved.Headers {
		req.Header.Del(key)
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}
}

func forwardSelectedHeaders(dst, src http.Header, keys ...string) {
	for _, key := range keys {
		for _, value := range src.Values(key) {
			dst.Add(key, value)
		}
	}
}

func unsupportedRouteBackend(route *gateway.Route) error {
	return fmt.Errorf("route %s uses unsupported backend %q for provider %s", route.Name, route.Backend, route.Provider)
}

func unsupportedProjectLocation(route *gateway.Route) error {
	return fmt.Errorf("route %s project/location routing is not supported by the configured provider proxy", route.Name)
}

func genericUpstreamError(status int) error {
	return withUpstreamDisposition(gateway.NewError(status, "upstream_error", "upstream_error", fmt.Sprintf("upstream returned %d", status)), status)
}

func withUpstreamDisposition(err *gateway.APIError, status int) error {
	switch {
	case status < http.StatusOK || (status >= http.StatusMultipleChoices && status < http.StatusBadRequest) || status > 599:
		return gateway.NewError(http.StatusBadGateway, "upstream_error", "unexpected_upstream_status", "upstream returned an unexpected HTTP status").
			WithDisposition(false, true, true)
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return gateway.NewError(http.StatusBadGateway, "upstream_error", "upstream_authentication_failed", "upstream route authentication failed").
			WithDisposition(false, true, true)
	case status == http.StatusNotFound && routeLocalNotFound(err):
		return gateway.NewError(http.StatusBadGateway, "upstream_error", "upstream_route_not_found", "upstream route or model was not found").
			WithDisposition(false, true, true)
	case status >= http.StatusInternalServerError:
		return err.WithDisposition(true, true, true)
	case status == http.StatusRequestTimeout || status == http.StatusConflict || status == http.StatusTooEarly:
		return err.WithDisposition(true, true, false)
	case status == http.StatusTooManyRequests:
		return err.WithDisposition(false, true, false)
	default:
		return err
	}
}

func routeLocalNotFound(err *gateway.APIError) bool {
	if err == nil {
		return false
	}
	value := strings.ToLower(err.Code + " " + err.Message)
	for _, marker := range []string{"model", "deployment", "endpoint"} {
		if strings.Contains(value, marker) {
			return true
		}
	}
	return false
}

func fallbackUsage(req *gateway.Request, routeEstimate gateway.Usage) gateway.Usage {
	if !routeEstimate.IsZero() {
		if routeEstimate.TotalTokens == 0 {
			routeEstimate.TotalTokens = saturatingProxyAdd(routeEstimate.InputTokens, routeEstimate.OutputTokens)
		}
		return routeEstimate
	}
	text := strings.TrimSpace(req.PromptText())
	in := req.Hints.EstimatedInputTokens
	if in == 0 && req.Meta.BodyBytes > 0 {
		in = req.Meta.BodyBytes / 4
		if in == 0 {
			in = 1
		}
	}
	if in == 0 && text != "" {
		in = int64(len([]rune(text))/4 + 1)
	}
	if in == 0 {
		in = 1
	}
	out := int64(req.RequestedMaxOutputTokens())
	if out == 0 && req.Operation != gateway.OpEmbeddings {
		out = 256
	}
	if req.Operation == gateway.OpEmbeddings {
		out = 0
	}
	return gateway.Usage{
		InputTokens:  in,
		OutputTokens: out,
		TotalTokens:  saturatingProxyAdd(in, out),
	}
}

func readBounded(reader io.Reader, limit int64) ([]byte, bool, error) {
	data, err := io.ReadAll(io.LimitReader(reader, saturatingProxyAdd(limit, 1)))
	if err != nil {
		return nil, false, err
	}
	if int64(len(data)) > limit {
		return nil, true, nil
	}
	return data, false, nil
}

func saturatingProxyAdd(first, second int64) int64 {
	if first < 0 {
		first = 0
	}
	if second < 0 {
		second = 0
	}
	if first > math.MaxInt64-second {
		return math.MaxInt64
	}
	return first + second
}

func upstreamModel(route *gateway.Route) string {
	if route == nil {
		return ""
	}
	if route.UpstreamModel != "" {
		return route.UpstreamModel
	}
	return route.Model
}

func patchBody(body []byte, overrides map[string]any) ([]byte, error) {
	raw := map[string]json.RawMessage{}
	if len(body) > 0 {
		if err := json.Unmarshal(body, &raw); err != nil {
			return nil, err
		}
	}
	for key, value := range overrides {
		if value == nil {
			delete(raw, key)
			continue
		}
		payload, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		raw[key] = payload
	}
	return json.Marshal(raw)
}

func joinURL(base, path string) string {
	return strings.TrimRight(base, "/") + "/" + strings.TrimLeft(path, "/")
}

func cloneHeaders(in http.Header) http.Header {
	if in == nil {
		return nil
	}
	return in.Clone()
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
