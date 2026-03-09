# flink-reactor-dsl

Core DSL library + CLI for FlinkReactor. React-style TSX DSL that synthesizes to Flink SQL + Kubernetes FlinkDeployment CRDs.

## What This Repo Contains

- `src/` — Core DSL engine, components, codegen, CLI, testing utilities
- `packages/create-fr-app/` — Project scaffolder (`create-fr-app`)
- `packages/ts-plugin/` — TypeScript language service plugin
- `scripts/` — Build and publish scripts

## Architecture Rules

- **Synthesis only** — no runtime code executes inside Flink. We generate SQL strings and YAML.
- **Custom JSX, not React** — `createElement()` builds a construct tree, not a virtual DOM.
- **DAG, not tree** — pipelines are directed acyclic graphs. JSX nesting is sugar for the linear case only.
- **Deterministic output** — same input must always produce the same SQL and YAML.
- **Flink SQL is the target** — all components compile to Flink SQL (v0.1). No DataStream API.

## Code Conventions

- TypeScript strict mode, no `any`
- Component files: `.ts` (they export node factories, not JSX components)
- Pipeline entry points: `pipelines/*/index.tsx` (these use JSX)
- Generated SQL: backtick-quote all identifiers
- Flink types: uppercase strings (`'BIGINT'`, `'TIMESTAMP(3)'`)
- No default exports except pipeline entry points and config files
- Tests: Vitest, snapshot tests for SQL output (`toMatchSnapshot()`)

## Commands

```bash
pnpm build          # Build with tsup
pnpm test           # Run tests with vitest
pnpm lint           # Biome check
pnpm format         # Biome format
pnpm changeset      # Create changeset
pnpm release        # Publish to npm
pnpm local:publish  # Publish to local Verdaccio
```

## Related Repositories

| Repo | Purpose | License |
|------|---------|---------|
| **`flink-reactor-dsl`** | **Core DSL + CLI (this repo)** | **BSL 1.1** |
| `flink-reactor-console` | Dashboard + Server | BSL 1.1 |
| `flink-reactor-platform` | Docs + orchestration | BSL 1.1 |
| `flink-reactor-specs` | Specifications | BSL 1.1 |
