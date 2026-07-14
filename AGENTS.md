# Repository guide for contributors and coding agents

## Purpose and invariants

`llmgw` is a compact Go LLM gateway with OpenAI-compatible and provider-native HTTP surfaces. Keep the data plane small, explicit, and safe under concurrency.

- The module is `llmgw`; the minimum supported Go version is 1.24.
- Keep one binary and the existing package boundaries. Avoid dynamic plugin loading, reflection-heavy abstractions, and giant provider interfaces.
- Public protocols are provider-native. Do not silently translate requests between OpenAI, Anthropic, and Gemini provider families.
- `gateway.Request` is the canonical routing and policy envelope, not a unified provider payload. It preserves the original JSON in `RawBody` and carries derived, provider-neutral policy facts in `RequestHints`.
- Configuration snapshots are immutable after loading and atomically swapped on reload. Never mutate a loaded `gateway.Snapshot` or its routes.
- Request interceptors run once per logical request; attempt interceptors run for every retry or fallback. Keep accounting and side effects in the correct ring.
- SQL must not be queried per inference request. PostgreSQL refreshes quota-limit snapshots; Redis or memory owns hot-path counters and reservations.

## Sources of truth

- [README.md](README.md) describes running, architecture, limits, and provider extension points.
- [docs/index.mdx](docs/index.mdx) is the user-documentation entry point.
- [docs/concepts/architecture.mdx](docs/concepts/architecture.mdx), [docs/concepts/request-lifecycle.mdx](docs/concepts/request-lifecycle.mdx), and [docs/concepts/provider-registry.mdx](docs/concepts/provider-registry.mdx) describe the implemented design.
- [config/config.example.yaml](config/config.example.yaml) is the checked-in configuration example and input to the OpenAPI snapshot.
- Code and tests take precedence when prose drifts; update the prose in the same change.

## Package ownership

- `cmd/llmgw`: CLI entry point and process lifecycle.
- `app`: service wiring, provider/store assembly, reload, and shutdown.
- `api`: the built-in ingress/adapter binding list, ingress registration, provider-native authentication and decoding, response/error encoding, operational endpoints, and OpenAPI generation.
- `gateway`: shared types, immutable config, routing, provider contracts, and request/attempt pipeline orchestration.
- `policy`: auth, metadata and token validation, ACLs, quota handling, attempt limits, timeouts, retries, and circuit breaking.
- `proxy`: generic upstream HTTP runtime plus provider protocol adapters, compatibility bridges, response validation, usage extraction, and SSE handling.
- `store`: in-memory and Redis hot-path state plus PostgreSQL quota-limit snapshot refresh.
- `observer`: structured logs, VictoriaMetrics-compatible metrics, and the small tracer hook interface.
- `test`: cross-package unit, end-to-end, and performance coverage. Package-local implementation tests may remain beside the implementation.

## Adding or changing a provider

Provider behavior has three explicit seams:

1. Add or update an `api.Ingress` for public paths, authentication, request decoding, error envelopes, and path matching.
2. Add or update a `proxy.Adapter`. Put operation preparation, upstream auth, error parsing, usage extraction, stream handling, bridges, token projection, and route validation in the adapter rather than provider-name switches.
3. Add the ingress and adapter factory pair once to `api.builtInProtocolBindings`. `api.DefaultIngresses` and `api.DefaultProviders` are derived from that list; do not create another provider-name registry. `gateway.ValidateProviders` must reject incompatible config at startup and before reload.

Use the shared OpenAI adapter for genuinely OpenAI-compatible upstreams. Add focused decoder, adapter, routing, streaming, and invalid-response tests whenever protocol behavior changes.

## Hot-path and security rules

- Use `context.Context` for cancellation and deadlines; do not leak goroutines or response bodies.
- Authenticate before reading request bodies. Keep request, response, and error bodies bounded.
- Preserve redirect refusal and credential-host validation in the proxy. Never forward gateway credentials upstream or log tokens, authorization headers, raw prompts, or secret configuration values.
- Keep routing provider-agnostic: filter by operation and declared capabilities, then let adapters preflight provider-specific constraints.
- A fallback may occur only before response bytes are committed. Preserve provider-native JSON/SSE and settle usage exactly once.
- Treat missing optional pricing as non-fatal unless an enabled hard-spend policy requires complete reservation metadata.
- Avoid panics in normal request flow and return the ingress protocol's error envelope.

## Generated OpenAPI and documentation

Do not hand-edit generated OpenAPI copies. After API or schema changes, run:

```bash
tmp="$(mktemp ./openapi.yaml.XXXXXX)"
trap 'rm -f "$tmp"' EXIT
LLMGW_BEARER_TOKEN=openapi-placeholder \
OPENAI_API_KEY=openapi-placeholder \
ANTHROPIC_API_KEY=openapi-placeholder \
GEMINI_API_KEY=openapi-placeholder \
  go run ./cmd/llmgw -config config/config.example.yaml -print-openapi > "$tmp"
mv "$tmp" openapi.yaml
trap - EXIT
cd docs
npm ci
npm run sync:openapi
npm run validate
npm run broken-links
npm run a11y
```

`docs/openapi.yaml` is derived from the root snapshot with only a docs-local server URL. Commit both generated snapshots when they change.

## Required checks

Before handing off a code change, run the checks relevant to it; for a repository-wide change, match CI:

```bash
gofmt -w <changed-go-files>
go test ./...
go test -race ./...
go vet ./...
go mod tidy -diff
go run honnef.co/go/tools/cmd/staticcheck@v0.7.0 ./...
go run golang.org/x/vuln/cmd/govulncheck@v1.6.0 ./...
git diff --check
```

Do not weaken tests or security checks merely to make validation pass. Do not commit, push, or alter unrelated working-tree changes unless explicitly asked.
