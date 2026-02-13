// Automatic JSX runtime entry point for tools like jiti/babel/esbuild.
// When tsconfig or transpiler uses "jsx": "react-jsx" with
// "jsxImportSource": "flink-reactor", this module is loaded automatically.

export { jsx, jsxs, Fragment } from './core/jsx-runtime.js';
