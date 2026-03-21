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

## Release & Publishing

Releases are automated via GitHub Actions — you never publish manually from your machine.

### How it works

This repo uses [Changesets](https://github.com/changesets/changesets) to manage versioning and publishing. The workflow has two phases:

**1. Create a changeset (you do this locally)**

When your PR includes a user-facing change, run:

```bash
pnpm changeset
```

This prompts you to select the affected packages and the semver bump type (patch, minor, or major). It creates a markdown file in `.changeset/` describing the change. Commit this file with your PR.

> Not every PR needs a changeset — skip it for internal refactors, CI changes, or docs-only updates that don't affect published packages.

**2. Merge to `main` (GitHub Actions takes over)**

When your PR merges, the [Release workflow](.github/workflows/release.yml) runs automatically:

```
PR merges to main
       │
       ▼
┌─────────────────────────────────┐
│  Pending changesets exist?      │
│                                 │
│  YES → Opens/updates a         │
│        "Version Packages" PR    │
│        (bumps versions +        │
│         updates changelogs)     │
│                                 │
│  NO  → Publishes all packages   │
│        to npm                   │
└─────────────────────────────────┘
```

- When changesets accumulate, the action maintains a **"Version Packages" PR** that batches all pending version bumps and changelog updates.
- When that PR is merged (no more pending changesets), the action **publishes to npm**.

### What gets published

| Package | npm name | Location |
|---------|----------|----------|
| Core DSL | `@flink-reactor/dsl` | root |
| Scaffolder | `@flink-reactor/create-fr-app` | `packages/create-fr-app/` |
| TS Plugin | `@flink-reactor/ts-plugin` | `packages/ts-plugin/` |

The root package (`@flink-reactor/dsl`) is published separately via [`scripts/publish-root.mjs`](scripts/publish-root.mjs) because Changesets only auto-publishes workspace member packages under `packages/`.

### Local testing with Verdaccio

To test packages locally before a real release, publish to a local [Verdaccio](https://verdaccio.org/) registry:

```bash
pnpm local:publish
```

This builds all packages, starts a Verdaccio server at `http://localhost:4873`, and publishes everything there. Install from it with:

```bash
npm install @flink-reactor/dsl --registry http://localhost:4873
```

### Commands reference

| Command | Description |
|---------|-------------|
| `pnpm changeset` | Create a new changeset |
| `pnpm version-packages` | Apply pending changesets (bump versions + changelogs) |
| `pnpm release` | Publish to npm (run by CI, not locally) |
| `pnpm local:publish` | Publish to local Verdaccio for testing |

## Code of Conduct

This project follows the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code.
