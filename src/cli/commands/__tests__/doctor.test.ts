import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { afterEach, beforeEach, describe, expect, it } from "vitest"
import {
  checkDockerVersion,
  checkNodeVersion,
  checkProjectConfig,
  checkTypeScriptVersion,
  discoverPipelines,
} from "@/cli/commands/doctor.js"

describe("doctor checks", () => {
  describe("checkNodeVersion", () => {
    it("returns pass when Node.js is available", () => {
      const result = checkNodeVersion()
      expect(result.name).toBe("Node.js")
      expect(result.status).toBe("pass")
      expect(result.version).toBeDefined()
    })
  })

  describe("checkTypeScriptVersion", () => {
    it("returns a result for TypeScript", () => {
      const result = checkTypeScriptVersion()
      expect(result.name).toBe("TypeScript")
      // May pass or fail depending on environment
      expect(["pass", "fail"]).toContain(result.status)
    })
  })

  describe("checkDockerVersion", () => {
    it("returns a result for Docker", () => {
      const result = checkDockerVersion()
      expect(result.name).toBe("Docker")
      expect(["pass", "warn"]).toContain(result.status)
    })
  })

  describe("checkProjectConfig", () => {
    let tempDir: string

    beforeEach(() => {
      tempDir = mkdtempSync(join(tmpdir(), "flink-reactor-doctor-"))
    })

    afterEach(() => {
      rmSync(tempDir, { recursive: true, force: true })
    })

    it("returns pass when config exists", () => {
      writeFileSync(
        join(tempDir, "flink-reactor.config.ts"),
        "export default {}",
      )
      const result = checkProjectConfig(tempDir)
      expect(result.status).toBe("pass")
    })

    it("returns warn when config is missing", () => {
      const result = checkProjectConfig(tempDir)
      expect(result.status).toBe("warn")
      expect(result.hint).toBeDefined()
    })
  })

  describe("discoverPipelines", () => {
    let tempDir: string

    beforeEach(() => {
      tempDir = mkdtempSync(join(tmpdir(), "flink-reactor-doctor-"))
    })

    afterEach(() => {
      rmSync(tempDir, { recursive: true, force: true })
    })

    it("returns warn when no pipelines directory", () => {
      const result = discoverPipelines(tempDir)
      expect(result.status).toBe("warn")
      expect(result.message).toContain("No pipelines/")
    })

    it("returns warn when pipelines directory is empty", () => {
      mkdirSync(join(tempDir, "pipelines"))
      const result = discoverPipelines(tempDir)
      expect(result.status).toBe("warn")
      expect(result.message).toContain("0 pipelines")
    })

    it("discovers pipelines with index.tsx", () => {
      const pipelineDir = join(tempDir, "pipelines", "my-pipeline")
      mkdirSync(pipelineDir, { recursive: true })
      writeFileSync(join(pipelineDir, "index.tsx"), "export default null;")

      const result = discoverPipelines(tempDir)
      expect(result.status).toBe("pass")
      expect(result.message).toContain("1 pipeline")
    })

    it("counts multiple pipelines", () => {
      for (const name of ["orders", "analytics", "notifications"]) {
        const dir = join(tempDir, "pipelines", name)
        mkdirSync(dir, { recursive: true })
        writeFileSync(join(dir, "index.tsx"), "export default null;")
      }

      const result = discoverPipelines(tempDir)
      expect(result.status).toBe("pass")
      expect(result.message).toContain("3 pipelines")
    })
  })
})
