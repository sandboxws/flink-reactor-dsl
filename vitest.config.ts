import { resolve } from "node:path"
import { defineConfig } from "vitest/config"

export default defineConfig({
  resolve: {
    alias: { "@": resolve(__dirname, "src") },
  },
  test: {
    globals: true,
    environment: "node",
    include: ["src/**/*.test.ts"],
    typecheck: {
      enabled: true,
      include: ["src/**/*.test-d.{ts,tsx}"],
    },
    coverage: {
      provider: "v8",
      include: ["src/**/*.ts"],
      exclude: ["src/**/*.test.ts", "src/**/*.d.ts"],
    },
  },
})
