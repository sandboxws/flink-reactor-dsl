import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { afterEach, beforeEach, describe, expect, it } from "vitest"
import type { ScaffoldOptions } from "@/cli/commands/new.js"
import { scaffoldProject } from "@/cli/commands/new.js"

function makeOptions(overrides?: Partial<ScaffoldOptions>): ScaffoldOptions {
  return {
    projectName: "test-project",
    template: "starter",
    pm: "pnpm",
    flinkVersion: "2.0",
    gitInit: false,
    installDeps: false,
    ...overrides,
  }
}

describe("scaffoldProject", () => {
  let tempDir: string
  let projectDir: string

  beforeEach(() => {
    tempDir = mkdtempSync(join(tmpdir(), "flink-reactor-test-"))
    projectDir = join(tempDir, "test-project")
  })

  afterEach(() => {
    rmSync(tempDir, { recursive: true, force: true })
  })

  describe("starter template", () => {
    it("creates expected file structure", () => {
      scaffoldProject(projectDir, makeOptions({ template: "starter" }))

      expect(existsSync(join(projectDir, "package.json"))).toBe(true)
      expect(existsSync(join(projectDir, "tsconfig.json"))).toBe(true)
      expect(existsSync(join(projectDir, "flink-reactor.config.ts"))).toBe(true)
      expect(existsSync(join(projectDir, ".gitignore"))).toBe(true)
      expect(existsSync(join(projectDir, "env", "dev.ts"))).toBe(true)
      expect(existsSync(join(projectDir, "schemas", "events.ts"))).toBe(true)
      expect(
        existsSync(join(projectDir, "pipelines", "hello-world", "index.tsx")),
      ).toBe(true)
      expect(
        existsSync(
          join(projectDir, "tests", "pipelines", "hello-world.test.ts"),
        ),
      ).toBe(true)
    })

    it("generates valid package.json", () => {
      scaffoldProject(projectDir, makeOptions({ template: "starter" }))

      const pkg = JSON.parse(
        readFileSync(join(projectDir, "package.json"), "utf-8"),
      )
      expect(pkg.name).toBe("test-project")
      expect(pkg.dependencies["flink-reactor"]).toBeDefined()
    })

    it("includes dashboard dependency and scripts", () => {
      scaffoldProject(projectDir, makeOptions({ template: "starter" }))

      const pkg = JSON.parse(
        readFileSync(join(projectDir, "package.json"), "utf-8"),
      )
      expect(pkg.dependencies["@flink-reactor/dashboard"]).toBe("^0.1.0")
      expect(pkg.scripts.dashboard).toBe("flink-reactor-dashboard start")
      expect(pkg.scripts["dashboard:mock"]).toBe(
        "flink-reactor-dashboard start --mock",
      )
    })

    it("includes flink version in config", () => {
      scaffoldProject(
        projectDir,
        makeOptions({ template: "starter", flinkVersion: "2.0" }),
      )

      const config = readFileSync(
        join(projectDir, "flink-reactor.config.ts"),
        "utf-8",
      )
      expect(config).toContain("'2.0'")
    })

    it("generates pipeline with JSX content", () => {
      scaffoldProject(projectDir, makeOptions({ template: "starter" }))

      const pipeline = readFileSync(
        join(projectDir, "pipelines", "hello-world", "index.tsx"),
        "utf-8",
      )
      expect(pipeline).toContain("<Pipeline")
      expect(pipeline).toContain("<KafkaSource")
      expect(pipeline).toContain("<KafkaSink")
    })
  })

  describe("minimal template", () => {
    it("creates minimal file structure", () => {
      scaffoldProject(projectDir, makeOptions({ template: "minimal" }))

      expect(existsSync(join(projectDir, "package.json"))).toBe(true)
      expect(existsSync(join(projectDir, "tsconfig.json"))).toBe(true)
      expect(existsSync(join(projectDir, "flink-reactor.config.ts"))).toBe(true)
      expect(existsSync(join(projectDir, "schemas", ".gitkeep"))).toBe(true)
      expect(existsSync(join(projectDir, "pipelines", ".gitkeep"))).toBe(true)
    })

    it("does not include dashboard dependency", () => {
      scaffoldProject(projectDir, makeOptions({ template: "minimal" }))

      const pkg = JSON.parse(
        readFileSync(join(projectDir, "package.json"), "utf-8"),
      )
      expect(pkg.dependencies["@flink-reactor/dashboard"]).toBeUndefined()
      expect(pkg.scripts.dashboard).toBeUndefined()
    })
  })

  describe("cdc-lakehouse template", () => {
    it("creates CDC pipeline with Debezium source", () => {
      scaffoldProject(projectDir, makeOptions({ template: "cdc-lakehouse" }))

      const pipeline = readFileSync(
        join(projectDir, "pipelines", "cdc-to-lakehouse", "index.tsx"),
        "utf-8",
      )
      expect(pipeline).toContain("debezium-json")
      expect(pipeline).toContain("<PaimonSink")
    })
  })

  describe("realtime-analytics template", () => {
    it("creates analytics pipeline with windowed aggregation", () => {
      scaffoldProject(
        projectDir,
        makeOptions({ template: "realtime-analytics" }),
      )

      const pipeline = readFileSync(
        join(projectDir, "pipelines", "page-view-analytics", "index.tsx"),
        "utf-8",
      )
      expect(pipeline).toContain("<TumbleWindow")
      expect(pipeline).toContain("<Aggregate")
      expect(pipeline).toContain("<JdbcSink")
    })
  })

  describe("monorepo template", () => {
    it("creates workspace structure", () => {
      scaffoldProject(projectDir, makeOptions({ template: "monorepo" }))

      expect(existsSync(join(projectDir, "pnpm-workspace.yaml"))).toBe(true)
      expect(
        existsSync(join(projectDir, "packages", "schemas", "package.json")),
      ).toBe(true)
      expect(
        existsSync(join(projectDir, "packages", "patterns", "package.json")),
      ).toBe(true)
      expect(
        existsSync(join(projectDir, "apps", "default-app", "package.json")),
      ).toBe(true)
      expect(
        existsSync(
          join(projectDir, "apps", "default-app", "flink-reactor.config.ts"),
        ),
      ).toBe(true)
    })

    it("uses workspace protocol in dependencies", () => {
      scaffoldProject(projectDir, makeOptions({ template: "monorepo" }))

      const patternsPkg = JSON.parse(
        readFileSync(
          join(projectDir, "packages", "patterns", "package.json"),
          "utf-8",
        ),
      )
      expect(patternsPkg.dependencies["@test-project/schemas"]).toBe(
        "workspace:*",
      )
    })

    it("includes dashboard as root devDependency", () => {
      scaffoldProject(projectDir, makeOptions({ template: "monorepo" }))

      const pkg = JSON.parse(
        readFileSync(join(projectDir, "package.json"), "utf-8"),
      )
      expect(pkg.devDependencies["@flink-reactor/dashboard"]).toBe("^0.1.0")
      expect(pkg.scripts.dashboard).toBe("flink-reactor-dashboard start")
      expect(pkg.scripts["dashboard:mock"]).toBe(
        "flink-reactor-dashboard start --mock",
      )
    })
  })

  describe("registry option", () => {
    it("creates .npmrc when registry is provided", () => {
      scaffoldProject(
        projectDir,
        makeOptions({ registry: "http://localhost:4873" }),
      )

      const npmrc = readFileSync(join(projectDir, ".npmrc"), "utf-8")
      expect(npmrc).toBe("registry=http://localhost:4873\n")
    })

    it("does not create .npmrc when registry is not provided", () => {
      scaffoldProject(projectDir, makeOptions())

      expect(existsSync(join(projectDir, ".npmrc"))).toBe(false)
    })
  })
})
