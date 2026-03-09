# flink-reactor-dsl

React-style TSX DSL that synthesizes to Flink SQL + Kubernetes FlinkDeployment CRDs.

## Install

```bash
npm install flink-reactor
```

## Quick Start

```bash
npx create-fr-app my-pipeline
cd my-pipeline
npm run synth
```

## Packages

| Package | Description |
|---------|-------------|
| `flink-reactor` | Core DSL library + CLI |
| `@flink-reactor/create-app` | Project scaffolder |
| `@flink-reactor/ts-plugin` | TypeScript language service plugin |

## License

BSL 1.1
