import { describe, expect, it } from "vitest"
import type { ScaffoldOptions, TemplateFile } from "@/cli/commands/new.js"
import { getPgFlussPaimonTemplates } from "@/cli/templates/pg-fluss-paimon.js"

const baseOpts: ScaffoldOptions = {
  projectName: "demo",
  template: "pg-fluss-paimon",
  pm: "pnpm",
  flinkVersion: "2.0",
  gitInit: false,
  installDeps: false,
}

function findFile(files: TemplateFile[], path: string): TemplateFile {
  // pg-fluss-paimon emits sharedFiles' generic config first, then overrides
  // it with a template-specific block — last-write-wins matches the
  // scaffolder's filesystem behavior (see scaffoldProject in new.ts).
  const matches = files.filter((f) => f.path === path)
  if (matches.length === 0) throw new Error(`No template file at path: ${path}`)
  return matches[matches.length - 1]
}

describe("pg-fluss-paimon template", () => {
  describe("flink-reactor.config.ts", () => {
    const files = getPgFlussPaimonTemplates(baseOpts)
    const config = findFile(files, "flink-reactor.config.ts")

    it("matches the locked-in snapshot", () => {
      expect(config.content).toMatchSnapshot()
    })

    it("declares a `test` environment targeting minikube", () => {
      // Load-bearing assertions — guard against silent edits via `vitest -u`.
      expect(config.content).toContain("test: {")
      expect(config.content).toContain("runtime: 'minikube'")
    })

    it("does not pre-provision Fluss/Paimon databases via sim.init", () => {
      // The Docker (`fr cluster up`) and minikube (`fr sim up`) lanes both
      // ship the runtime ready to consume — Fluss CDC creates databases on
      // first record and Paimon's serve SQL emits CREATE DATABASE/TABLE
      // IF NOT EXISTS. Pre-provisioning here would re-introduce the
      // benchmark-specific naming the template explicitly drops.
      expect(config.content).not.toContain("databases: ['benchmark']")
      expect(config.content).not.toContain("databases: ['public']")
    })

    it("preserves the production environment without sim.init (real cluster)", () => {
      const prodIdx = config.content.indexOf("production: {")
      const prodBlock = config.content.slice(prodIdx)
      expect(prodBlock).toContain("kubernetes: { namespace: 'flink-prod' }")
      expect(prodBlock).not.toContain("sim:")
    })
  })

  describe("README.md", () => {
    const files = getPgFlussPaimonTemplates(baseOpts)
    const readme = findFile(files, "README.md")

    it("matches the locked-in snapshot", () => {
      expect(readme.content).toMatchSnapshot()
    })

    it("includes a Quickstart section showing the three-step golden path", () => {
      expect(readme.content).toContain("## Quickstart")
      expect(readme.content).toContain("pnpm install")
      expect(readme.content).toContain("pnpm fr cluster up --runtime=docker")
      expect(readme.content).toContain("pnpm fr sim up")
      expect(readme.content).toContain(
        "pnpm fr deploy ingest && pnpm fr deploy serve",
      )
    })

    it("enumerates the runtime resources both lanes ship", () => {
      expect(readme.content).toContain("fluss_catalog")
      expect(readme.content).toContain("Paimon warehouse")
      expect(readme.content).toContain("`tpch`")
      expect(readme.content).toContain("`flink_cdc`")
      expect(readme.content).toContain("`flink_cdc_pub`")
    })

    it("documents the docker-compose lane as a first-class option", () => {
      expect(readme.content).toContain("pnpm fr cluster up --runtime=docker")
    })

    it("documents the optional Grafana opt-in snippet", () => {
      // The pg-fluss-paimon template stays opt-out by default, but the
      // README must show users how to enable Grafana so the bundled
      // `pg-fluss-paimon` dashboard panels actually populate.
      expect(readme.content).toContain("Optional: enable Grafana metrics")
      expect(readme.content).toContain(
        "services: { postgres: {}, fluss: {}, grafana: {} }",
      )
      expect(readme.content).toContain("metricsPlugin({ reporters:")
      expect(readme.content).toContain("pnpm fr cluster open grafana")
    })
  })

  describe("with grafanaEnabled: true", () => {
    // When the user opts in via `fr new --grafana` (or the interactive
    // prompt on Flink 2.x), the rendered config must wire up *both* the
    // service entry and the Prometheus reporter plugin. Half-config
    // produces a Grafana dashboard with no data — worse than no opt-in.
    const files = getPgFlussPaimonTemplates({
      ...baseOpts,
      grafanaEnabled: true,
    })
    const config = findFile(files, "flink-reactor.config.ts")

    it("imports metricsPlugin from the dsl plugins entry-point", () => {
      expect(config.content).toContain(
        "import { metricsPlugin } from '@flink-reactor/dsl/plugins'",
      )
    })

    it("adds grafana to the existing services block (preserving postgres + fluss)", () => {
      expect(config.content).toContain(
        "services: { postgres: {}, fluss: {}, grafana: {} }",
      )
    })

    it("registers metricsPlugin with the Prometheus reporter on port 9249", () => {
      expect(config.content).toContain("plugins: [")
      expect(config.content).toContain(
        "metricsPlugin({ reporters: [{ type: 'prometheus', port: 9249 }] })",
      )
    })

    it("matches the locked-in snapshot for the opt-in path", () => {
      expect(config.content).toMatchSnapshot()
    })
  })

  describe("with grafanaEnabled: false", () => {
    // The default path must remain byte-identical to the legacy output
    // so existing scaffolds aren't churned by passing through the new
    // option.
    const files = getPgFlussPaimonTemplates({
      ...baseOpts,
      grafanaEnabled: false,
    })
    const config = findFile(files, "flink-reactor.config.ts")

    it("does not import metricsPlugin", () => {
      expect(config.content).not.toContain("metricsPlugin")
    })

    it("ships the legacy services block (postgres + fluss only)", () => {
      expect(config.content).toContain("services: { postgres: {}, fluss: {} }")
      expect(config.content).not.toContain("grafana: {}")
    })

    it("does not include a plugins array", () => {
      expect(config.content).not.toContain("plugins:")
    })
  })
})
