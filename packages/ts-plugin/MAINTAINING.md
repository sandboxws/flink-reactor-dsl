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

## Module Architecture

```
src/
├── index.ts              ← Entry point, wires modules together
├── service.ts            ← Language service proxy creation
├── diagnostics.ts        ← Nesting diagnostic logic
├── completions.ts        ← Completion filtering by context
├── component-rules.ts    ← Hierarchy rules registry
├── component-inventory.ts ← Canonical DSL component list (source of truth)
├── context-detector.ts   ← JSX tag name extraction
├── diagnostic-codes.ts   ← Standardized codes and messages
├── parity-check.ts       ← Rule/inventory drift detection
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
| `context-detector.test.ts` | Tag name extraction | Yes |
| `diagnostics.test.ts` | Nesting validation | Yes |
| `compatibility.test.ts` | Refactor regression guard | Yes |
| `parity.test.ts` | DSL ↔ plugin sync | Yes |
| `parity-check.test.ts` | Drift detection module | Yes |
| `diagnostic-codes.test.ts` | Code conventions | Yes |
| `release-checks.test.ts` | Package metadata/structure | Yes |
