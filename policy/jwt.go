package policy

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
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
	key, err := jwtVerificationKey(cfg)
	if err != nil {
		return nil, gateway.NewError(http.StatusInternalServerError, "server_error", "jwt_config_invalid", err.Error())
	}
	claims := jwt.MapClaims{}
	options := []jwt.ParserOption{jwt.WithValidMethods([]string{cfg.Algorithm})}
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
	principal := gateway.Principal{
		ID:        firstNonEmpty(claimString(claims, cfg.Claims.Principal), claimString(claims, "sub")),
		Name:      claimString(claims, cfg.Claims.Name),
		KeyID:     claimString(claims, cfg.Claims.KeyID),
		Providers: claimStrings(claims, cfg.Claims.Providers),
		Models:    claimStrings(claims, cfg.Claims.Models),
		Projects:  claimStrings(claims, cfg.Claims.Projects),
	}
	if principal.ID == "" {
		principal.ID = principal.KeyID
	}
	return &principal, nil
}

func jwtVerificationKey(cfg gateway.JWTConfig) (any, error) {
	switch strings.ToUpper(cfg.Algorithm) {
	case "HS256", "HS384", "HS512":
		if cfg.Secret == "" {
			return nil, fmt.Errorf("jwt secret is required for %s", cfg.Algorithm)
		}
		return []byte(cfg.Secret), nil
	case "RS256", "RS384", "RS512":
		return parsePEMPublicKey[*rsa.PublicKey](cfg.PublicKey)
	case "ES256", "ES384", "ES512":
		return parsePEMPublicKey[*ecdsa.PublicKey](cfg.PublicKey)
	case "EDDSA":
		return parsePEMPublicKey[ed25519.PublicKey](cfg.PublicKey)
	default:
		return nil, fmt.Errorf("unsupported jwt algorithm %q", cfg.Algorithm)
	}
}

func parsePEMPublicKey[T any](value string) (T, error) {
	var zero T
	if strings.TrimSpace(value) == "" {
		return zero, fmt.Errorf("jwt public key is required")
	}
	block, _ := pem.Decode([]byte(value))
	if block == nil {
		return zero, fmt.Errorf("invalid jwt public key PEM")
	}
	if key, err := x509.ParsePKIXPublicKey(block.Bytes); err == nil {
		if typed, ok := key.(T); ok {
			return typed, nil
		}
		return zero, fmt.Errorf("unexpected jwt public key type")
	}
	legacy, err := legacyPublicKey[T](block.Bytes)
	if err != nil {
		return zero, err
	}
	typed, ok := legacy.(T)
	if !ok {
		return zero, fmt.Errorf("unexpected jwt public key type")
	}
	return typed, nil
}

func legacyPublicKey[T any](der []byte) (any, error) {
	var zero T
	switch any(zero).(type) {
	case *rsa.PublicKey:
		return x509.ParsePKCS1PublicKey(der)
	default:
		return nil, fmt.Errorf("unsupported jwt public key encoding")
	}
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

func claimStrings(claims jwt.MapClaims, key string) []string {
	if key == "" {
		return nil
	}
	switch value := claims[key].(type) {
	case string:
		return splitClaimList(value)
	case []string:
		return sanitizeClaimList(value)
	case []any:
		out := make([]string, 0, len(value))
		for _, item := range value {
			if text, ok := item.(string); ok && strings.TrimSpace(text) != "" {
				out = append(out, strings.TrimSpace(text))
			}
		}
		return out
	default:
		return nil
	}
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
