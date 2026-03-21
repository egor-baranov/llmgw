package api_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	gwapi "llmgw/api"
	"llmgw/gateway"
	"llmgw/store"

	"github.com/golang-jwt/jwt/v5"
)

func TestQuotaLimitsMethodNotAllowed(t *testing.T) {
	rr := httptest.NewRecorder()
	req := quotaRequest(t, http.MethodPost, "/v1/limits", "", signedQuotaJWT(t, "test-secret", jwt.MapClaims{
		"iss":    "llmgw-tests",
		"aud":    "gateway",
		"sub":    "principal-1",
		"key_id": "key-1",
	}))

	newQuotaServer(&quotaLimitStub{}, nil).Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusMethodNotAllowed)
	}
	if got := rr.Header().Get("Allow"); got != "GET, PUT" {
		t.Fatalf("allow = %q, want GET, PUT", got)
	}
	assertErrorCode(t, rr.Body.String(), "method_not_allowed")
}

func TestQuotaLimitsGetPropagatesLookupFailure(t *testing.T) {
	rr := httptest.NewRecorder()
	req := quotaRequest(t, http.MethodGet, "/v1/limits", "", signedQuotaJWT(t, "test-secret", jwt.MapClaims{
		"iss":    "llmgw-tests",
		"aud":    "gateway",
		"sub":    "principal-1",
		"key_id": "key-1",
	}))

	newQuotaServer(&quotaLimitStub{getErr: errors.New("boom")}, nil).Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusInternalServerError)
	}
	assertErrorCode(t, rr.Body.String(), "quota_limit_lookup_failed")
}

func TestQuotaLimitsGetPropagatesUsageFailure(t *testing.T) {
	rr := httptest.NewRecorder()
	req := quotaRequest(t, http.MethodGet, "/v1/limits", "", signedQuotaJWT(t, "test-secret", jwt.MapClaims{
		"iss":    "llmgw-tests",
		"aud":    "gateway",
		"sub":    "principal-1",
		"key_id": "key-1",
	}))

	newQuotaServer(&quotaLimitStub{limits: map[string]gateway.LimitSpec{
		"key-1": {RPM: 10},
	}}, &quotaUsageStub{err: errors.New("boom")}).Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusInternalServerError)
	}
	assertErrorCode(t, rr.Body.String(), "quota_usage_lookup_failed")
}

func TestQuotaLimitsPutPropagatesWriteFailure(t *testing.T) {
	rr := httptest.NewRecorder()
	req := quotaRequest(t, http.MethodPut, "/v1/limits", `{"rpm":10}`, signedQuotaJWT(t, "test-secret", jwt.MapClaims{
		"iss":    "llmgw-tests",
		"aud":    "gateway",
		"sub":    "principal-1",
		"key_id": "key-1",
	}))

	newQuotaServer(&quotaLimitStub{putErr: errors.New("boom")}, nil).Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusInternalServerError)
	}
	assertErrorCode(t, rr.Body.String(), "quota_limit_write_failed")
}

func TestQuotaLimitsGetReturnsNotFoundWithoutConfiguredLimits(t *testing.T) {
	rr := httptest.NewRecorder()
	req := quotaRequest(t, http.MethodGet, "/v1/limits", "", signedQuotaJWT(t, "test-secret", jwt.MapClaims{
		"iss":    "llmgw-tests",
		"aud":    "gateway",
		"sub":    "principal-1",
		"key_id": "key-1",
	}))

	newQuotaServer(nil, nil).Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusNotFound)
	}
	assertErrorCode(t, rr.Body.String(), "quota_limits_not_found")
}

func newQuotaServer(limits store.QuotaLimitStore, usage store.QuotaUsageStore) *gwapi.Server {
	return gwapi.NewServer(
		nil,
		gateway.NewConfigStore(&gateway.Snapshot{
			Auth: gateway.AuthConfig{
				JWT: gateway.JWTConfig{
					Algorithm: "HS256",
					Issuer:    "llmgw-tests",
					Audience:  "gateway",
					Secret:    "test-secret",
				},
			},
		}),
		nil,
		limits,
		usage,
	)
}

func quotaRequest(t *testing.T, method, path, body, token string) *http.Request {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	return req
}

func signedQuotaJWT(t *testing.T, secret string, claims jwt.MapClaims) string {
	t.Helper()
	if _, ok := claims["exp"]; !ok {
		claims["exp"] = time.Now().Add(time.Hour).Unix()
	}
	if _, ok := claims["iat"]; !ok {
		claims["iat"] = time.Now().Unix()
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString([]byte(secret))
	if err != nil {
		t.Fatalf("SignedString() error = %v", err)
	}
	return signed
}

func assertErrorCode(t *testing.T, body, want string) {
	t.Helper()
	var payload struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal([]byte(body), &payload); err != nil {
		t.Fatalf("json.Unmarshal(%s) error = %v", body, err)
	}
	if payload.Error.Code != want {
		t.Fatalf("error code = %q, want %q", payload.Error.Code, want)
	}
}

type quotaLimitStub struct {
	limits map[string]gateway.LimitSpec
	getErr error
	putErr error
}

func (s *quotaLimitStub) Get(_ context.Context, keyID string) (gateway.LimitSpec, bool, error) {
	if s == nil {
		return gateway.LimitSpec{}, false, nil
	}
	if s.getErr != nil {
		return gateway.LimitSpec{}, false, s.getErr
	}
	limit, ok := s.limits[keyID]
	return limit, ok, nil
}

func (s *quotaLimitStub) Put(_ context.Context, keyID string, limit gateway.LimitSpec) error {
	if s == nil {
		return nil
	}
	if s.putErr != nil {
		return s.putErr
	}
	if s.limits == nil {
		s.limits = map[string]gateway.LimitSpec{}
	}
	s.limits[keyID] = limit
	return nil
}

type quotaUsageStub struct {
	err error
}

func (s *quotaUsageStub) GetUsage(context.Context, gateway.ScopedLimit) (gateway.QuotaUsage, error) {
	if s != nil && s.err != nil {
		return gateway.QuotaUsage{}, s.err
	}
	return gateway.QuotaUsage{}, nil
}
