package gateway

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"strings"
)

// VerificationKey parses and validates the configured JWT verification key.
// Keeping this validation with the configuration model lets startup and reload
// reject unusable keys before the service advertises readiness.
func (j JWTConfig) VerificationKey() (any, error) {
	if j.verificationKey != nil {
		return j.verificationKey, nil
	}
	j = j.Normalize()
	switch j.Algorithm {
	case "HS256", "HS384", "HS512":
		if strings.TrimSpace(j.Secret) == "" {
			return nil, fmt.Errorf("jwt secret is required for %s", j.Algorithm)
		}
		minimumBytes := map[string]int{
			"HS256": 32,
			"HS384": 48,
			"HS512": 64,
		}[j.Algorithm]
		if len(j.Secret) < minimumBytes {
			return nil, fmt.Errorf("%s requires a jwt secret of at least %d bytes (got %d)", j.Algorithm, minimumBytes, len(j.Secret))
		}
		return []byte(j.Secret), nil
	case "RS256", "RS384", "RS512":
		key, err := parseJWTPublicKey[*rsa.PublicKey](j.PublicKey)
		if err != nil {
			return nil, err
		}
		if key == nil || key.N == nil {
			return nil, fmt.Errorf("invalid RSA public key")
		}
		if bits := key.N.BitLen(); bits < 2048 {
			return nil, fmt.Errorf("%s requires an RSA key of at least 2048 bits (got %d)", j.Algorithm, bits)
		}
		return key, nil
	case "ES256", "ES384", "ES512":
		key, err := parseJWTPublicKey[*ecdsa.PublicKey](j.PublicKey)
		if err != nil {
			return nil, err
		}
		if err := validateECDSACurve(j.Algorithm, key); err != nil {
			return nil, err
		}
		return key, nil
	case "EdDSA":
		key, err := parseJWTPublicKey[ed25519.PublicKey](j.PublicKey)
		if err != nil {
			return nil, err
		}
		if len(key) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("invalid Ed25519 public key length")
		}
		return key, nil
	default:
		return nil, fmt.Errorf("unsupported jwt algorithm %q", j.Algorithm)
	}
}

func parseJWTPublicKey[T any](value string) (T, error) {
	var zero T
	if strings.TrimSpace(value) == "" {
		return zero, fmt.Errorf("jwt public key is required")
	}
	block, rest := pem.Decode([]byte(value))
	if block == nil || len(strings.TrimSpace(string(rest))) != 0 {
		return zero, fmt.Errorf("invalid jwt public key PEM")
	}
	if key, err := x509.ParsePKIXPublicKey(block.Bytes); err == nil {
		if typed, ok := key.(T); ok {
			return typed, nil
		}
		return zero, fmt.Errorf("jwt public key type is incompatible with the configured algorithm")
	}
	legacy, err := legacyJWTPublicKey[T](block.Bytes)
	if err != nil {
		return zero, err
	}
	typed, ok := legacy.(T)
	if !ok {
		return zero, fmt.Errorf("jwt public key type is incompatible with the configured algorithm")
	}
	return typed, nil
}

func legacyJWTPublicKey[T any](der []byte) (any, error) {
	var zero T
	switch any(zero).(type) {
	case *rsa.PublicKey:
		key, err := x509.ParsePKCS1PublicKey(der)
		if err != nil {
			return nil, fmt.Errorf("invalid RSA public key: %w", err)
		}
		return key, nil
	default:
		return nil, fmt.Errorf("invalid jwt public key encoding")
	}
}

func validateECDSACurve(algorithm string, key *ecdsa.PublicKey) error {
	if key == nil || key.Curve == nil || key.Curve.Params() == nil {
		return fmt.Errorf("invalid ECDSA public key")
	}
	var expected elliptic.Curve
	switch algorithm {
	case "ES256":
		expected = elliptic.P256()
	case "ES384":
		expected = elliptic.P384()
	case "ES512":
		expected = elliptic.P521()
	default:
		return fmt.Errorf("unsupported ECDSA jwt algorithm %q", algorithm)
	}
	if key.Curve.Params().Name != expected.Params().Name {
		return fmt.Errorf("%s requires curve %s, got %s", algorithm, expected.Params().Name, key.Curve.Params().Name)
	}
	return nil
}
