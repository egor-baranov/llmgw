package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"llmgw/gateway"
)

func writeJSON(w http.ResponseWriter, status int, payload any) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	return json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, err error) {
	apiErr := gateway.AsAPIError(err)
	_ = writeJSON(w, apiErr.Status, map[string]any{
		"error": map[string]any{
			"message": apiErr.Message,
			"type":    apiErr.Type,
			"param":   apiErr.Param,
			"code":    apiErr.Code,
		},
	})
}

func copyProviderErrorHeaders(dst, src http.Header) {
	for key, values := range src {
		lower := strings.ToLower(key)
		switch {
		case strings.EqualFold(key, "X-Request-ID"):
			if len(values) > 0 {
				dst.Set("X-LLMGW-Upstream-Request-ID", values[len(values)-1])
			}
		case strings.EqualFold(key, "Request-ID"):
			if len(values) > 0 {
				dst.Set("Request-ID", values[len(values)-1])
				dst.Set("X-LLMGW-Upstream-Request-ID", values[len(values)-1])
			}
		case strings.EqualFold(key, "Retry-After"),
			strings.HasPrefix(lower, "x-ratelimit-"),
			strings.HasPrefix(lower, "ratelimit-"),
			strings.HasPrefix(lower, "anthropic-ratelimit-"):
			for _, value := range values {
				dst.Add(key, value)
			}
		}
	}
}

func writeProviderResult(w http.ResponseWriter, result *gateway.Result, writeIdleTimeout time.Duration) (gateway.Usage, error) {
	if result == nil || len(result.RawBody) == 0 {
		return gateway.Usage{}, gateway.NewError(http.StatusBadGateway, "server_error", "empty_result", "provider returned no raw response body")
	}
	if writeIdleTimeout <= 0 {
		writeIdleTimeout = 30 * time.Second
	}
	controller := http.NewResponseController(w)
	if err := controller.SetWriteDeadline(time.Now().Add(writeIdleTimeout)); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return result.FinalUsage(), err
	}
	defer func() { _ = controller.SetWriteDeadline(time.Time{}) }()
	copyProxyHeaders(w.Header(), result.Headers)
	if result.ContentType != "" {
		w.Header().Set("Content-Type", result.ContentType)
	}
	w.WriteHeader(statusOrDefault(result.StatusCode))
	_, err := w.Write(result.RawBody)
	return result.Usage, err
}

func writeProviderStream(w http.ResponseWriter, result *gateway.Result, onFirstByte func(), writeIdleTimeout time.Duration) (gateway.Usage, error) {
	if result == nil || result.RawStream == nil {
		return gateway.Usage{}, gateway.UnsupportedOperation("provider-native raw stream is not available")
	}
	defer result.RawStream.Close()
	flusher, ok := w.(http.Flusher)
	if !ok {
		return result.FinalUsage(), gateway.NewError(http.StatusInternalServerError, "server_error", "streaming_unsupported", "response writer does not support streaming")
	}
	copyProxyHeaders(w.Header(), result.Headers)
	w.Header().Del("Content-Length")
	if result.ContentType != "" {
		w.Header().Set("Content-Type", result.ContentType)
	}
	if writeIdleTimeout <= 0 {
		writeIdleTimeout = 30 * time.Second
	}
	controller := http.NewResponseController(w)
	refreshWriteDeadline := func() error {
		err := controller.SetWriteDeadline(time.Now().Add(writeIdleTimeout))
		if errors.Is(err, http.ErrNotSupported) {
			return nil
		}
		return err
	}
	if err := refreshWriteDeadline(); err != nil {
		return result.FinalUsage(), err
	}
	defer func() { _ = controller.SetWriteDeadline(time.Time{}) }()
	w.WriteHeader(statusOrDefault(result.StatusCode))
	firstByteSent := false
	buf := make([]byte, 32*1024)
	for {
		n, err := result.RawStream.Read(buf)
		if n > 0 {
			if deadlineErr := refreshWriteDeadline(); deadlineErr != nil {
				return result.FinalUsage(), deadlineErr
			}
			if _, writeErr := w.Write(buf[:n]); writeErr != nil {
				return result.FinalUsage(), writeErr
			}
			if !firstByteSent {
				firstByteSent = true
				if onFirstByte != nil {
					onFirstByte()
				}
			}
			flusher.Flush()
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return result.FinalUsage(), err
		}
	}
	return result.FinalUsage(), nil
}

func copyProxyHeaders(dst, src http.Header) {
	connectionHeaders := make(map[string]struct{})
	for key, values := range src {
		if !strings.EqualFold(key, "Connection") {
			continue
		}
		for _, value := range values {
			for _, token := range strings.Split(value, ",") {
				if token = http.CanonicalHeaderKey(strings.TrimSpace(token)); token != "" {
					connectionHeaders[token] = struct{}{}
				}
			}
		}
	}
	for key, values := range src {
		_, nominatedByConnection := connectionHeaders[http.CanonicalHeaderKey(key)]
		if nominatedByConnection || isHopByHopHeader(key) || isSensitiveUpstreamResponseHeader(key) || strings.HasPrefix(strings.ToLower(key), "x-llmgw-") {
			continue
		}
		if strings.EqualFold(key, "X-Request-ID") {
			if len(values) > 0 {
				dst.Set("X-LLMGW-Upstream-Request-ID", values[len(values)-1])
			}
			continue
		}
		if strings.EqualFold(key, "X-LLMGW-Upstream-Request-ID") {
			continue
		}
		dst.Del(key)
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

// isSensitiveUpstreamResponseHeader rejects credentials and request-only
// metadata that a malicious or misconfigured upstream might echo. Ordinary
// response metadata, content headers, request IDs, and rate-limit headers remain
// available to clients.
func isSensitiveUpstreamResponseHeader(key string) bool {
	lower := strings.ToLower(strings.TrimSpace(key))
	switch lower {
	case "authorization", "proxy-authorization",
		"cookie", "cookie2", "set-cookie", "set-cookie2",
		"authentication-info", "proxy-authentication-info",
		"forwarded", "x-forwarded-for", "x-forwarded-host", "x-forwarded-proto", "x-forwarded-port",
		"x-real-ip", "x-client-ip", "x-cluster-client-ip", "true-client-ip", "cf-connecting-ip",
		"host", "origin", "referer", "user-agent",
		"openai-organization", "openai-project", "anthropic-version", "anthropic-beta",
		"x-goog-api-client", "x-goog-user-project",
		"x-amz-signature",
		"traceparent", "tracestate", "baggage", "x-ot-span-context", "uber-trace-id":
		return true
	}
	return lower == "key" || lower == "token" ||
		strings.HasSuffix(lower, "-key") || strings.HasSuffix(lower, "-token") ||
		strings.Contains(lower, "credential") || strings.Contains(lower, "secret")
}

func isHopByHopHeader(key string) bool {
	switch http.CanonicalHeaderKey(key) {
	case "Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "Proxy-Connection", "Te", "Trailer", "Transfer-Encoding", "Upgrade":
		return true
	default:
		return false
	}
}

func statusOrDefault(status int) int {
	if status == 0 {
		return http.StatusOK
	}
	return status
}
