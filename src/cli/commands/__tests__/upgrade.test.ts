import { mkdtempSync, rmSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { afterEach, beforeEach, describe, expect, it } from "vitest"
import { detectPackageManager, resolvePackageName } from "../upgrade.js"

describe("upgrade command", () => {
  let tempDir: string

  beforeEach(() => {
    tempDir = mkdtempSync(join(tmpdir(), "flink-reactor-upgrade-test-"))
  })

  afterEach(() => {
    rmSync(tempDir, { recursive: true, force: true })
  })

  describe("resolvePackageName", () => {
    it('resolves "dashboard" shorthand', () => {
      expect(resolvePackageName("dashboard")).toBe("@flink-reactor/dashboard")
    })

    it("passes through full package names unchanged", () => {
      expect(resolvePackageName("@flink-reactor/dashboard")).toBe(
        "@flink-reactor/dashboard",
      )
    })

    it("passes through unknown names unchanged", () => {
      expect(resolvePackageName("flink-reactor")).toBe("flink-reactor")
    })
  })

  describe("detectPackageManager", () => {
    it("detects pnpm from pnpm-lock.yaml", () => {
      writeFileSync(join(tempDir, "pnpm-lock.yaml"), "lockfileVersion: 9\n")

      expect(detectPackageManager(tempDir)).toBe("pnpm")
    })

    it("detects yarn from yarn.lock", () => {
      writeFileSync(join(tempDir, "yarn.lock"), "# yarn lockfile v1\n")

      expect(detectPackageManager(tempDir)).toBe("yarn")
    })

    it("detects npm from package-lock.json", () => {
      writeFileSync(join(tempDir, "package-lock.json"), "{}")

      expect(detectPackageManager(tempDir)).toBe("npm")
    })

    it("defaults to npm when no lockfile exists", () => {
      expect(detectPackageManager(tempDir)).toBe("npm")
    })

    it("prefers pnpm when both pnpm-lock.yaml and package-lock.json exist", () => {
      writeFileSync(join(tempDir, "pnpm-lock.yaml"), "lockfileVersion: 9\n")
      writeFileSync(join(tempDir, "package-lock.json"), "{}")

      expect(detectPackageManager(tempDir)).toBe("pnpm")
    })
  })
})
