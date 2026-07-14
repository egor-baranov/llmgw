# llmgw documentation site

The documentation UI uses [Mintlify](https://www.mintlify.com/docs). All site source, navigation, and local tooling live in this directory; no custom documentation frontend is maintained by the project.

## Run locally

Node.js 18 or newer is sufficient for the npm commands. Mintlify itself does not
support Node.js 25+, so this project installs and uses a pinned Node.js 24 runtime
for Mint commands automatically. You do not need to change or downgrade your
global Node.js installation.

```bash
cd docs
npm ci
npm run dev
```

Mintlify opens the site at <http://localhost:3000>. Use `npm run dev:no-open` when running it in a terminal or container where the browser should not open automatically.

If dependencies were installed before the pinned runtime was added, run `npm ci`
once before starting the site again.

The pinned runtime is installed by npm's lifecycle scripts. Do not use
`npm ci --ignore-scripts`; restricted or mirrored registries must also provide
the platform-specific package selected by the `node` dependency.

The service itself is separate. Run llmgw at <http://localhost:8080> when following examples or testing requests from another terminal.

## Validate changes

```bash
cd docs
npm run validate
npm run broken-links
npm run a11y
```

The npm commands synchronize `docs/openapi.yaml` from the checked-in specification at the repository root before starting or validating the site. The sync adds only a docs-local `http://localhost:8080` server so generated request examples have the correct local base URL. The root snapshot is verified by the Go test suite, so endpoint schemas remain aligned with the service.

## Content conventions

- Keep operational claims aligned with the Go implementation and `config/config.example.yaml`.
- Prefer runnable examples using placeholder credentials.
- Never place production credentials in docs, screenshots, or API examples.
- Add every user-facing MDX page to `docs.json` navigation.
- Keep endpoint details in the generated OpenAPI reference and use prose pages for workflows and explanations.
