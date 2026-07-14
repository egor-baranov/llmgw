package api

import (
	"net/http"
	"strings"

	"llmgw/gateway"
	"llmgw/policy"
	"llmgw/proxy"
)

// RequestDecoder validates a provider-native request and returns the canonical
// metadata used by the gateway while preserving the original request body.
type RequestDecoder func(r *http.Request, maxBytes int64) (*gateway.Request, error)

// IngressAuthenticator owns the credential contract for one provider-facing
// ingress. It runs before request decoding so unauthorized bodies are never
// read.
type IngressAuthenticator func(cfg *gateway.Snapshot, r *http.Request) (*gateway.Principal, error)

// IngressErrorWriter preserves the public error envelope of an ingress.
type IngressErrorWriter func(w http.ResponseWriter, err error)

// IngressResolver supports path-driven operations such as Gemini's
// models/{model}:operation surface. Static routes can set Operation and Decoder
// directly instead.
type IngressResolver func(r *http.Request) (gateway.Operation, RequestDecoder, *http.Request, error)

// IngressRoute is one provider-owned HTTP registration. Pattern follows
// http.ServeMux semantics; Method is enforced before authentication or body
// decoding.
type IngressRoute struct {
	Pattern   string
	Method    string
	Operation gateway.Operation
	Decoder   RequestDecoder
	Resolve   IngressResolver
}

// Ingress describes one provider-native northbound protocol. The descriptor
// owns route registration, authentication, error formatting, and unknown-path
// matching. Fallback marks the envelope used when no other ingress matches.
type Ingress struct {
	Provider     string
	Routes       []IngressRoute
	Authenticate IngressAuthenticator
	WriteError   IngressErrorWriter
	MatchPath    func(path string) bool
	Fallback     bool
}

type protocolBinding struct {
	newIngress func() Ingress
	newAdapter func() proxy.Adapter
}

// builtInProtocolBindings is the composition root for checked-in provider
// protocols. Keeping the northbound ingress and upstream adapter factories in
// one entry makes it impossible to add a built-in to only one side of the
// service assembly by accident.
var builtInProtocolBindings = [...]protocolBinding{
	{newIngress: AnthropicIngress, newAdapter: proxy.AnthropicAdapter},
	{newIngress: GeminiIngress, newAdapter: proxy.GeminiAdapter},
	{newIngress: OpenAIIngress, newAdapter: proxy.OpenAIAdapter},
}

// DefaultIngresses returns fresh descriptors for every checked-in provider
// protocol. OpenAI remains the unknown-path fallback for backward
// compatibility.
func DefaultIngresses() []Ingress {
	ingresses := make([]Ingress, 0, len(builtInProtocolBindings))
	for _, binding := range builtInProtocolBindings {
		ingresses = append(ingresses, binding.newIngress())
	}
	return ingresses
}

// DefaultProviders returns fresh generic proxy runtimes for the same built-in
// protocols exposed by DefaultIngresses.
func DefaultProviders(client *http.Client) []gateway.Provider {
	providers := make([]gateway.Provider, 0, len(builtInProtocolBindings))
	for _, binding := range builtInProtocolBindings {
		providers = append(providers, proxy.NewProvider(binding.newAdapter(), client))
	}
	return providers
}

func (s *Server) configuredIngresses() []Ingress {
	if len(s.ingresses) == 0 {
		return DefaultIngresses()
	}
	return append([]Ingress(nil), s.ingresses...)
}

func (s *Server) registerIngresses(mux *http.ServeMux, ingresses []Ingress) {
	for _, ingress := range ingresses {
		ingress := ingress
		for _, route := range ingress.Routes {
			route := route
			mux.HandleFunc(route.Pattern, s.ingressRouteHandler(ingress, route))
		}
	}
}

func (s *Server) ingressRouteHandler(ingress Ingress, route IngressRoute) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != route.Method {
			w.Header().Set("Allow", route.Method)
			err := gateway.NewError(http.StatusMethodNotAllowed, "invalid_request_error", "method_not_allowed", "method not allowed")
			s.observeIngressError(r, ingress.providerName(), route.Operation, err)
			ingress.writeError(w, err)
			return
		}

		op, decoder, request, err := route.resolve(r)
		if op == "" {
			op = route.Operation
		}
		if err != nil {
			s.observeIngressError(r, ingress.providerName(), op, err)
			ingress.writeError(w, err)
			return
		}
		if decoder == nil {
			err = gateway.NewError(http.StatusInternalServerError, "server_error", "ingress_not_configured", "ingress request decoder is not configured")
			s.observeIngressError(r, ingress.providerName(), op, err)
			ingress.writeError(w, err)
			return
		}
		if request == nil {
			request = r
		}
		s.handleIngressOperation(ingress, op, decoder)(w, request)
	}
}

func (route IngressRoute) resolve(r *http.Request) (gateway.Operation, RequestDecoder, *http.Request, error) {
	if route.Resolve != nil {
		return route.Resolve(r)
	}
	if route.Decoder == nil {
		return route.Operation, nil, r, gateway.NewError(http.StatusInternalServerError, "server_error", "ingress_not_configured", "ingress request decoder is not configured")
	}
	return route.Operation, route.Decoder, r, nil
}

func (s *Server) notFoundHandler(ingresses []Ingress) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ingress := ingressForPath(ingresses, r.URL.Path)
		err := gateway.NewError(http.StatusNotFound, "invalid_request_error", "not_found", "endpoint not found")
		s.observeIngressError(r, ingress.providerName(), gateway.Operation("unknown"), err)
		ingress.writeError(w, err)
	}
}

func ingressForPath(ingresses []Ingress, path string) Ingress {
	for _, ingress := range ingresses {
		if !ingress.Fallback && ingress.matches(path) {
			return ingress
		}
	}
	for _, ingress := range ingresses {
		if ingress.matches(path) {
			return ingress
		}
	}
	for _, ingress := range ingresses {
		if ingress.Fallback {
			return ingress
		}
	}
	return Ingress{Provider: "unknown", Authenticate: bearerIngressAuthenticator(), WriteError: writeOpenAIError, Fallback: true}
}

func (ingress Ingress) matches(path string) bool {
	if ingress.MatchPath != nil {
		return ingress.MatchPath(path)
	}
	for _, route := range ingress.Routes {
		if strings.HasSuffix(route.Pattern, "/") {
			if strings.HasPrefix(path, route.Pattern) {
				return true
			}
		} else if path == route.Pattern {
			return true
		}
	}
	return false
}

func (ingress Ingress) providerName() string {
	if provider := strings.TrimSpace(ingress.Provider); provider != "" {
		return provider
	}
	return "unknown"
}

func (ingress Ingress) authenticate(cfg *gateway.Snapshot, r *http.Request) (*gateway.Principal, error) {
	if ingress.Authenticate != nil {
		return ingress.Authenticate(cfg, r)
	}
	return bearerIngressAuthenticator()(cfg, r)
}

func (ingress Ingress) writeError(w http.ResponseWriter, err error) {
	if ingress.WriteError != nil {
		ingress.WriteError(w, err)
		return
	}
	writeOpenAIError(w, err)
}

func bearerIngressAuthenticator(nativeHeader ...string) IngressAuthenticator {
	header := ""
	if len(nativeHeader) > 0 {
		header = nativeHeader[0]
	}
	return func(cfg *gateway.Snapshot, r *http.Request) (*gateway.Principal, error) {
		auth := strings.TrimSpace(r.Header.Get("Authorization"))
		token := ""
		presented := false
		if auth != "" {
			presented = true
			parts := strings.SplitN(auth, " ", 2)
			if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || strings.TrimSpace(parts[1]) == "" {
				return nil, gateway.Unauthorized("invalid authorization header")
			}
			token = strings.TrimSpace(parts[1])
		} else if header != "" {
			token = strings.TrimSpace(r.Header.Get(header))
			presented = token != ""
		}
		if !presented {
			if cfg.Auth.AllowAnonymous {
				return nil, nil
			}
			return nil, gateway.Unauthorized("missing bearer token")
		}
		return policy.AuthenticatePrincipal(cfg, token)
	}
}

func writeOpenAIError(w http.ResponseWriter, err error) {
	copyProviderErrorHeaders(w.Header(), gateway.AsAPIError(err).Headers)
	writeError(w, err)
}
