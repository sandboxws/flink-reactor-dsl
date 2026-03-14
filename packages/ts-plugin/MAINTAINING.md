# Maintaining the ts-plugin

How to keep the TypeScript plugin in sync when the DSL evolves.

## Adding a New DSL Component

When you add a new component to `src/components/` and register it in `BUILTIN_KINDS` (`src/core/jsx-runtime.ts`), follow these steps to prevent rule drift:

### 1. Update the component inventory

Add the component to `DSL_COMPONENTS` in `packages/ts-plugin/src/component-inventory.ts`:

```ts
["MyNewComponent", "Transform"],  // or Source, Sink, Join, etc.
```

### 2. Update the hierarchy rules

Add the component as an allowed child in `packages/ts-plugin/src/component-rules.ts`:

- **Top-level components** (sources, sinks, transforms, joins, windows): add to `Pipeline`'s allowed children array.
- **Sub-components** (e.g., `MyComponent.Sub`): add to the parent component's allowed children array, and add a wildcard or explicit rule for the sub-component itself.

### 3. Run parity tests

```bash
cd packages/ts-plugin && pnpm test
```

The parity tests (`__tests__/parity.test.ts`) will fail if:
- A component in the inventory is missing from Pipeline's rules
- A component in the rules is not in the inventory
- The inventory count changes (tripwire test — update the expected count)

### 4. Update the count tripwire

If you added components, update the expected count in `__tests__/parity.test.ts`:

```ts
expect(DSL_COMPONENTS.size).toBe(<new count>)
```

## Host Setup Guides

### Compatibility Matrix

| Host | TypeScript | Loading Path | Status |
|------|-----------|-------------|--------|
| VS Code | ≥ 5.0 | `compilerOptions.plugins` in tsconfig.json | Supported |
| Neovim `ts_ls` | ≥ 5.0 | `init_options.plugins` in LSP config | Supported |
| Neovim `vtsls` | ≥ 5.0 | `tsserver.globalPlugins` in vtsls settings | Supported |

### VS Code Setup

1. Install the plugin in your project:

```bash
pnpm add -D @flink-reactor/ts-plugin
```

2. Add to `tsconfig.json`:

```json
{
  "compilerOptions": {
    "plugins": [{ "name": "@flink-reactor/ts-plugin" }]
  }
}
```

3. Select workspace TypeScript version (required if VS Code bundles an older TS):
   - Open command palette: `Ctrl+Shift+P` / `Cmd+Shift+P`
   - Run: **TypeScript: Select TypeScript Version**
   - Choose: **Use Workspace Version**

4. Verify activation:
   - Open a `.tsx` file with FlinkReactor components
   - Check the TS server log (`Ctrl+Shift+U` → Output → TypeScript) for `[flink-reactor] Plugin initialized successfully`

### Neovim `ts_ls` Setup

1. Install the plugin in your project:

```bash
pnpm add -D @flink-reactor/ts-plugin
```

2. Configure `ts_ls` in your Neovim LSP setup (e.g., `lspconfig`):

```lua
require('lspconfig').ts_ls.setup({
  init_options = {
    plugins = {
      {
        name = "@flink-reactor/ts-plugin",
        location = vim.fn.getcwd() .. "/node_modules/@flink-reactor/ts-plugin",
      },
    },
  },
})
```

3. Ensure `tsconfig.json` includes the plugin (same as VS Code setup above).

4. Verify activation:
   - Open a `.tsx` file
   - Check `:LspLog` for `[flink-reactor] Plugin initialized successfully`

### Neovim `vtsls` Setup

1. Install the plugin in your project:

```bash
pnpm add -D @flink-reactor/ts-plugin
```

2. Configure `vtsls` in your Neovim LSP setup:

```lua
require('lspconfig').vtsls.setup({
  settings = {
    vtsls = {
      tsserver = {
        globalPlugins = {
          {
            name = "@flink-reactor/ts-plugin",
            location = vim.fn.getcwd() .. "/node_modules/@flink-reactor/ts-plugin",
            enableForWorkspaceTypeScriptVersions = true,
          },
        },
      },
    },
  },
})
```

3. Verify activation:
   - Open a `.tsx` file
   - Check `:LspLog` for `[flink-reactor] Plugin initialized successfully`

### Troubleshooting

**Plugin not activating (no `[flink-reactor]` log messages)**

- Confirm the plugin package is installed (`node_modules/@flink-reactor/ts-plugin` exists)
- Confirm `tsconfig.json` contains the plugin in `compilerOptions.plugins`
- VS Code: ensure workspace TypeScript is selected (not the bundled version)
- Neovim: ensure the `location` path resolves to the installed package directory
- Restart the TypeScript server after config changes

**Diagnostics not appearing in `.tsx` files**

- Ensure the file extension is `.tsx` (not `.ts`) — the plugin only activates for TSX files
- Check if `disableDiagnostics: true` is set in the plugin config
- Verify nesting: diagnostics only fire for invalid parent-child relationships in JSX

**Completions not context-aware**

- Ensure the file extension is `.tsx`
- Check if `disableCompletions: true` is set in the plugin config
- Cursor must be inside a JSX element's children region (not inside a tag or attribute)
- Default strategy is "rank" (reorders, doesn't remove) — check if "filter" strategy is preferred

**Plugin errors in server log**

- Errors are caught and logged with `ERROR in diagnostics/completions` prefix
- The plugin falls back to baseline TypeScript behavior on error
- Report the full error message if the issue persists

**TypeScript version warnings**

- The plugin requires TypeScript ≥ 5.0.0
- A warning is logged if a lower version is detected
- The plugin still attempts to load but behavior may be degraded

## Module Architecture

```
src/
├── index.ts              ← Entry point, wires modules together
├── service.ts            ← Language service proxy creation
├── diagnostics.ts        ← Nesting diagnostic logic
├── completions.ts        ← Completion filtering/ranking by context
├── component-rules.ts    ← Hierarchy rules registry
├── component-inventory.ts ← Canonical DSL component list (source of truth)
├── context-detector.ts   ← JSX tag name extraction + parent resolution
├── diagnostic-codes.ts   ← Standardized codes and messages
├── parity-check.ts       ← Rule/inventory drift detection
├── host-compatibility.ts ← Supported hosts, TS version policy
└── types.ts              ← Shared interfaces
```

## Adding a New Diagnostic

1. Add a code to `DiagnosticCodes` in `src/diagnostic-codes.ts` (90xxx range)
2. Add a message builder function in the same file
3. Use it in the relevant module (diagnostics, completions, etc.)
4. Add tests in `__tests__/diagnostic-codes.test.ts`

## Test Categories

| Test file | Purpose | Blocks release? |
|-----------|---------|:---:|
| `component-rules.test.ts` | Registry behavior | Yes |
| `completions.test.ts` | Context-aware completions | Yes |
| `context-detector.test.ts` | Tag name extraction | Yes |
| `diagnostics.test.ts` | Nesting validation | Yes |
| `compatibility.test.ts` | Refactor regression guard | Yes |
| `parity.test.ts` | DSL ↔ plugin sync | Yes |
| `parity-check.test.ts` | Drift detection module | Yes |
| `diagnostic-codes.test.ts` | Code conventions | Yes |
| `release-checks.test.ts` | Package metadata/structure | Yes |
| `host-compatibility.test.ts` | LSP host smoke tests | Yes |
