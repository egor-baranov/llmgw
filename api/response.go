package api

import (
	"encoding/json"
	"io"
	"net/http"

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

func writeProviderResult(w http.ResponseWriter, result *gateway.Result) (gateway.Usage, error) {
	if result == nil || len(result.RawBody) == 0 {
		return gateway.Usage{}, gateway.NewError(http.StatusBadGateway, "server_error", "empty_result", "provider returned no raw response body")
	}
	copyProxyHeaders(w.Header(), result.Headers)
	if result.ContentType != "" {
		w.Header().Set("Content-Type", result.ContentType)
	}
	w.WriteHeader(statusOrDefault(result.StatusCode))
	_, err := w.Write(result.RawBody)
	return result.Usage, err
}

func writeProviderStream(w http.ResponseWriter, result *gateway.Result, onFirstByte func()) (gateway.Usage, error) {
	if result == nil || result.RawStream == nil {
		return gateway.Usage{}, gateway.UnsupportedOperation("provider-native raw stream is not available")
	}
	defer result.RawStream.Close()
	copyProxyHeaders(w.Header(), result.Headers)
	if result.ContentType != "" {
		w.Header().Set("Content-Type", result.ContentType)
	}
	w.WriteHeader(statusOrDefault(result.StatusCode))
	flusher, ok := w.(http.Flusher)
	if !ok {
		return gateway.Usage{}, gateway.NewError(http.StatusInternalServerError, "server_error", "streaming_unsupported", "response writer does not support streaming")
	}
	firstByteSent := false
	buf := make([]byte, 32*1024)
	for {
		n, err := result.RawStream.Read(buf)
		if n > 0 {
			if _, writeErr := w.Write(buf[:n]); writeErr != nil {
				if !firstByteSent {
					return gateway.Usage{}, writeErr
				}
				return result.Usage, writeErr
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
			if !firstByteSent {
				return gateway.Usage{}, err
			}
			return result.Usage, err
		}
	}
	return result.Usage, nil
}

func copyProxyHeaders(dst, src http.Header) {
	for key, values := range src {
		if isHopByHopHeader(key) {
			continue
		}
		dst.Del(key)
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func isHopByHopHeader(key string) bool {
	switch http.CanonicalHeaderKey(key) {
	case "Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "Te", "Trailer", "Transfer-Encoding", "Upgrade":
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
