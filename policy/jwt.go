package policy

import (
	"fmt"
	"net/http"
	"strings"

	"llmgw/gateway"

	"github.com/golang-jwt/jwt/v5"
)

func AuthenticatePrincipal(snapshot *gateway.Snapshot, rawToken string) (*gateway.Principal, error) {
	if snapshot == nil {
		return nil, gateway.NewError(http.StatusServiceUnavailable, "server_error", "not_ready", "configuration not loaded")
	}
	if principal, ok := snapshot.Auth.Tokens[rawToken]; ok {
		return &principal, nil
	}
	if snapshot.Auth.JWT.Enabled() {
		return parseJWTPrincipal(snapshot.Auth.JWT, rawToken)
	}
	return nil, gateway.Unauthorized("invalid bearer token")
}

func parseJWTPrincipal(cfg gateway.JWTConfig, rawToken string) (*gateway.Principal, error) {
	cfg = cfg.Normalize()
	key, err := cfg.VerificationKey()
	if err != nil {
		return nil, gateway.NewError(http.StatusInternalServerError, "server_error", "jwt_config_invalid", "authentication configuration is invalid")
	}
	claims := jwt.MapClaims{}
	options := []jwt.ParserOption{
		jwt.WithValidMethods([]string{cfg.Algorithm}),
		jwt.WithExpirationRequired(),
	}
	if cfg.Issuer != "" {
		options = append(options, jwt.WithIssuer(cfg.Issuer))
	}
	if cfg.Audience != "" {
		options = append(options, jwt.WithAudience(cfg.Audience))
	}
	token, err := jwt.ParseWithClaims(rawToken, claims, func(token *jwt.Token) (any, error) {
		if token.Method == nil {
			return nil, fmt.Errorf("missing signing method")
		}
		return key, nil
	}, options...)
	if err != nil || token == nil || !token.Valid {
		return nil, gateway.Unauthorized("invalid jwt")
	}
	providers, err := claimStrings(claims, cfg.Claims.Providers, true)
	if err != nil {
		return nil, gateway.Unauthorized("jwt has invalid provider authorization claims")
	}
	models, err := claimStrings(claims, cfg.Claims.Models, true)
	if err != nil {
		return nil, gateway.Unauthorized("jwt has invalid model authorization claims")
	}
	projects, err := claimStrings(claims, cfg.Claims.Projects, true)
	if err != nil {
		return nil, gateway.Unauthorized("jwt has invalid project authorization claims")
	}
	permissions, err := claimStrings(claims, cfg.Claims.Permissions, false)
	if err != nil {
		return nil, gateway.Unauthorized("jwt has invalid permission claims")
	}
	principal := gateway.Principal{
		ID:          firstNonEmpty(claimString(claims, cfg.Claims.Principal), claimString(claims, "sub")),
		Name:        claimString(claims, cfg.Claims.Name),
		KeyID:       claimString(claims, cfg.Claims.KeyID),
		Providers:   providers,
		Models:      models,
		Projects:    projects,
		Permissions: permissions,
	}
	if principal.ID == "" {
		principal.ID = principal.KeyID
	}
	if principal.ID == "" {
		return nil, gateway.Unauthorized("jwt is missing a stable principal identity")
	}
	return &principal, nil
}

func claimString(claims jwt.MapClaims, key string) string {
	if key == "" {
		return ""
	}
	switch value := claims[key].(type) {
	case string:
		return strings.TrimSpace(value)
	case fmt.Stringer:
		return strings.TrimSpace(value.String())
	default:
		return ""
	}
}

func claimStrings(claims jwt.MapClaims, key string, requireNonEmpty bool) ([]string, error) {
	if key == "" {
		return nil, nil
	}
	raw, present := claims[key]
	if !present {
		return nil, nil
	}
	var out []string
	switch value := raw.(type) {
	case string:
		out = splitClaimList(value)
	case []string:
		out = sanitizeClaimList(value)
	case []any:
		out = make([]string, 0, len(value))
		for _, item := range value {
			text, ok := item.(string)
			if !ok || strings.TrimSpace(text) == "" {
				return nil, fmt.Errorf("claim %s must contain only non-empty strings", key)
			}
			out = append(out, strings.TrimSpace(text))
		}
	default:
		return nil, fmt.Errorf("claim %s must be a string or string array", key)
	}
	if requireNonEmpty && len(out) == 0 {
		return nil, fmt.Errorf("claim %s must not be empty", key)
	}
	return out, nil
}

func splitClaimList(value string) []string {
	parts := strings.Split(value, ",")
	return sanitizeClaimList(parts)
}

func sanitizeClaimList(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}
