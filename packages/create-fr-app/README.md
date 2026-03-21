<h1 align="center">@flink-reactor/create-fr-app</h1>

<p align="center">
  <strong>Scaffold a new FlinkReactor streaming pipeline project in seconds.</strong>
</p>

<p align="center">
  <a href="https://www.npmjs.com/package/@flink-reactor/create-fr-app"><img src="https://img.shields.io/npm/v/@flink-reactor/create-fr-app?color=d97085&label=npm" alt="npm version" /></a>
  <img src="https://img.shields.io/badge/license-MIT-green" alt="license" />
</p>

## Usage

```bash
# Create a new project (interactive)
npx @flink-reactor/create-fr-app

# Or with a project name
npx @flink-reactor/create-fr-app init my-pipeline

# Using the short alias
npx create-fr-app my-pipeline
```

## Commands

| Command | Description |
|---------|-------------|
| `init [name]` | Create a new FlinkReactor project (default when no command given) |
| `add <components...>` | Add components to an existing project |
| `check` | Check for available component updates |
| `update` | Update components to latest versions |

## Options

```
init [project-name]
  -t, --template <template>  Template to use (default: "nextjs")
  --no-git                   Skip git initialization
  --no-install               Skip dependency installation
```

## What it scaffolds

```
my-pipeline/
├── pipelines/
│   └── my-pipeline/
│       └── index.tsx        # Your first pipeline
├── schemas/                 # Shared schema definitions
├── fr.config.ts             # FlinkReactor configuration
├── tsconfig.json            # TypeScript config with JSX
└── package.json
```

## Links

- [FlinkReactor Documentation](https://flink-reactor.dev)
- [GitHub Repository](https://github.com/sandboxws/flink-reactor-dsl)
- [Report Issues](https://github.com/sandboxws/flink-reactor-dsl/issues)
