<h1 align="center">@flink-reactor/ts-plugin</h1>

<p align="center">
  <strong>TypeScript language service plugin for FlinkReactor JSX diagnostics.</strong>
</p>

<p align="center">
  <a href="https://www.npmjs.com/package/@flink-reactor/ts-plugin"><img src="https://img.shields.io/npm/v/@flink-reactor/ts-plugin?color=d97085&label=npm" alt="npm version" /></a>
  <img src="https://img.shields.io/badge/license-MIT-green" alt="license" />
</p>

## What it provides

- **JSX nesting diagnostics** — validates that FlinkReactor components are nested correctly (e.g., `<Aggregate>` inside `<TumbleWindow>`, sources before transforms)
- **Context-aware completions** — suggests valid child components based on the current JSX context

## Install

```bash
npm install -D @flink-reactor/ts-plugin
```

## Setup

Add the plugin to your `tsconfig.json`:

```json
{
  "compilerOptions": {
    "plugins": [
      { "name": "@flink-reactor/ts-plugin" }
    ]
  }
}
```

### VS Code

VS Code uses its own bundled TypeScript by default. To use the workspace version (which loads plugins):

1. Open Command Palette (`Cmd+Shift+P` / `Ctrl+Shift+P`)
2. Run **TypeScript: Select TypeScript Version**
3. Choose **Use Workspace Version**

## Links

- [FlinkReactor Documentation](https://flink-reactor.dev)
- [GitHub Repository](https://github.com/sandboxws/flink-reactor-dsl)
- [Report Issues](https://github.com/sandboxws/flink-reactor-dsl/issues)
