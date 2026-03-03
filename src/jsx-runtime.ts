// Automatic JSX runtime entry point for tools like jiti/babel/esbuild.
// When tsconfig or transpiler uses "jsx": "react-jsx" with
// "jsxImportSource": "flink-reactor", this module is loaded automatically.

export { Fragment, jsx, jsxs } from "./core/jsx-runtime.js"
