# Contributing to FlinkReactor

We welcome contributions of all kinds — bug reports, feature suggestions, documentation improvements, and code.

## Development Setup

```bash
# Clone the repository
git clone https://github.com/sandboxws/flink-reactor-dsl.git
cd flink-reactor-dsl

# Install dependencies
pnpm install

# Build all packages
pnpm build
```

## Running Tests

```bash
# Run all tests
pnpm test

# Run tests in watch mode
pnpm test:watch

# Run snapshot tests only
pnpm test:snapshots

# Type-check without emitting
pnpm typecheck
```

## Code Style

This project uses [Biome](https://biomejs.dev/) for linting and formatting, enforced automatically by pre-commit hooks.

```bash
# Check for lint issues
pnpm lint

# Auto-fix lint issues
pnpm lint:fix

# Format code
pnpm format
```

Pre-commit hooks run automatically — you don't need to remember to lint before committing.

## Making Changes

1. **Fork the repo** and create a branch from `main`
2. **Make your changes** — keep commits focused and use conventional commit messages (`feat:`, `fix:`, `docs:`, `refactor:`)
3. **Add a changeset** if your change affects published packages:
   ```bash
   pnpm changeset
   ```
4. **Run tests** to make sure nothing breaks:
   ```bash
   pnpm test && pnpm typecheck
   ```
5. **Open a pull request** against `main`

## CLA Requirement

First-time contributors will be asked to sign our [Contributor License Agreement](.github/CLA.md) via a PR comment. This is a one-time process.

## Architecture

See the [Architecture section](README.md#-architecture) in the README for an overview of how the DSL, construct tree, and code generators work together.

Key principles:
- **Synthesis only** — no runtime code. We generate SQL strings and YAML.
- **Custom JSX** — `createElement()` builds a construct tree, not React.
- **Deterministic output** — same input always produces the same SQL and YAML.

## Project Structure

| Directory | Purpose |
|-----------|---------|
| `src/core/` | JSX runtime, schemas, synth context, DAG |
| `src/components/` | Sources, sinks, transforms, joins, windows |
| `src/codegen/` | SQL generator, CRD generator, JAR resolution |
| `src/cli/` | CLI commands |
| `src/testing/` | Test helpers |
| `packages/create-fr-app/` | Project scaffolder |
| `packages/ts-plugin/` | TypeScript language service plugin |

## Code of Conduct

This project follows the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code.
