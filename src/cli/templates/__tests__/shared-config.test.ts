// Unit tests for `makeConfig()` — the shared rendering of
// `flink-reactor.config.ts` consumed by every template that doesn't
// override its own config (so ~14 of 17 templates).
//
// The existence (or absence) of Grafana wiring in the rendered output is
// load-bearing: the `services.grafana: {}` entry activates the
// `observability` Compose profile at `cluster up`, and the `metricsPlugin`
// registration is what makes Prometheus see Flink JM/TM metrics. Both
// must travel together — half-config (one without the other) silently
// produces empty Grafana panels.

import { describe, expect, it } from "vitest"
import type { ScaffoldOptions } from "@/cli/commands/new.js"
import { injectGrafanaWiring, makeConfig } from "@/cli/templates/shared.js"

const baseOpts: ScaffoldOptions = {
  projectName: "demo",
  template: "starter",
  pm: "pnpm",
  flinkVersion: "2.2",
  gitInit: false,
  installDeps: false,
}

describe("makeConfig — default (no Grafana)", () => {
  const config = makeConfig(baseOpts)

  it("does not import metricsPlugin", () => {
    expect(config).not.toContain("metricsPlugin")
    expect(config).not.toContain("@flink-reactor/dsl/plugins")
  })

  it("renders an empty services block", () => {
    expect(config).toContain("services: {}")
  })

  it("does not render a plugins array", () => {
    expect(config).not.toContain("plugins:")
  })

  it("renders the user-selected Flink version", () => {
    expect(config).toContain("flink: { version: '2.2' }")
  })
})

describe("makeConfig — with grafanaEnabled: true", () => {
  const config = makeConfig({ ...baseOpts, grafanaEnabled: true })

  it("imports metricsPlugin from the dsl plugins entry-point", () => {
    expect(config).toContain(
      "import { metricsPlugin } from '@flink-reactor/dsl/plugins'",
    )
  })

  it("renders services.grafana: {} so the observability Compose profile activates", () => {
    expect(config).toContain("services: { grafana: {} }")
  })

  it("registers metricsPlugin with the Prometheus reporter on port 9249", () => {
    // Port 9249 matches the Prometheus scrape target wired in
    // src/cli/cluster/observability/prometheus/prometheus.yml.
    expect(config).toContain(
      "metricsPlugin({ reporters: [{ type: 'prometheus', port: 9249 }] })",
    )
  })

  it("preserves the user-selected Flink version", () => {
    expect(config).toContain("flink: { version: '2.2' }")
  })
})

describe("makeConfig — with grafanaEnabled: true on Flink 2.0", () => {
  // The CLI gating in `fr new` only offers Grafana for 2.x, but
  // `makeConfig` itself is unconditional — it just renders what it's told.
  // Verify the rendering works for any 2.x version.
  const config = makeConfig({
    ...baseOpts,
    flinkVersion: "2.0",
    grafanaEnabled: true,
  })

  it("renders Grafana wiring for Flink 2.0", () => {
    expect(config).toContain("flink: { version: '2.0' }")
    expect(config).toContain("services: { grafana: {} }")
    expect(config).toContain("metricsPlugin")
  })
})

describe("injectGrafanaWiring — post-processor for hand-rolled template configs", () => {
  // Most templates emit their own `flink-reactor.config.ts` rather than
  // going through `makeConfig`. The post-processor in scaffoldProject
  // wires Grafana into those configs uniformly — these tests cover the
  // shapes templates actually emit.

  it("returns input unchanged when enabled is false/undefined", () => {
    const input = `import { defineConfig } from '@flink-reactor/dsl';\n\nexport default defineConfig({ services: {} });`
    expect(injectGrafanaWiring(input, false)).toBe(input)
    expect(injectGrafanaWiring(input, undefined)).toBe(input)
  })

  it("injects wiring into a starter-shaped config (kafka services, no plugins)", () => {
    const input = `import { defineConfig } from '@flink-reactor/dsl';

export default defineConfig({
  flink: { version: '2.2' },
  services: { kafka: { bootstrapServers: 'kafka:9092' } },

  environments: {
    development: { cluster: { url: 'http://localhost:8081' } },
  },
});
`
    const out = injectGrafanaWiring(input, true)
    expect(out).toContain(
      "import { metricsPlugin } from '@flink-reactor/dsl/plugins'",
    )
    expect(out).toContain(
      "services: { kafka: { bootstrapServers: 'kafka:9092' }, grafana: {} }",
    )
    expect(out).toContain(
      "metricsPlugin({ reporters: [{ type: 'prometheus', port: 9249 }] })",
    )
  })

  it("injects into an empty services block", () => {
    const input = `import { defineConfig } from '@flink-reactor/dsl'

export default defineConfig({
  flink: { version: '2.2' },
  services: {},

  environments: {
    development: {},
  },
})
`
    const out = injectGrafanaWiring(input, true)
    expect(out).toContain("services: { grafana: {} }")
    expect(out).toContain("metricsPlugin")
  })

  it("is idempotent — re-running on already-wired config is a no-op", () => {
    const input = `import { defineConfig } from '@flink-reactor/dsl'
import { metricsPlugin } from '@flink-reactor/dsl/plugins'

export default defineConfig({
  flink: { version: '2.2' },
  services: { postgres: {}, fluss: {}, grafana: {} },

  plugins: [
    metricsPlugin({ reporters: [{ type: 'prometheus', port: 9249 }] }),
  ],

  environments: {},
})
`
    const out = injectGrafanaWiring(input, true)
    // No double-import, no double-grafana, no double-plugin entry.
    expect((out.match(/import \{ metricsPlugin \}/g) ?? []).length).toBe(1)
    expect((out.match(/grafana: \{\}/g) ?? []).length).toBe(1)
    expect((out.match(/metricsPlugin\(\{ reporters/g) ?? []).length).toBe(1)
  })

  it("prepends to an existing plugins array rather than replacing it", () => {
    const input = `import { defineConfig } from '@flink-reactor/dsl'
import { customPlugin } from './plugins/custom.js'

export default defineConfig({
  services: {},
  plugins: [
    customPlugin({ foo: 'bar' }),
  ],
  environments: {},
})
`
    const out = injectGrafanaWiring(input, true)
    expect(out).toContain("metricsPlugin({ reporters:")
    expect(out).toContain("customPlugin({ foo: 'bar' })")
  })
})
