// Development JSX runtime entry point.
// Bundlers (esbuild/vite) emit `import { jsxDEV } from "<jsxImportSource>/jsx-dev-runtime"`
// when `jsx: "react-jsx"` is set and NODE_ENV !== "production" (e.g. under vitest).
// Our DSL doesn't consume the dev-only `source`/`self` debug args, so `jsxDEV`
// is a thin alias over `jsx`.

export { Fragment, jsx as jsxDEV, jsxs } from "./core/jsx-runtime.js"
