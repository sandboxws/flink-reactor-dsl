import { describe, expect, it } from "vitest"
import { FlinkVersionCompat } from "@/core/flink-compat.js"

describe("FlinkVersionCompat.normalizeConfig", () => {
  it("returns config as-is for Flink 2.0+", () => {
    const config = {
      "state.backend.type": "rocksdb",
      "execution.checkpointing.interval": "30s",
    }

    const result = FlinkVersionCompat.normalizeConfig(config, "2.0")
    expect(result).toEqual(config)
  })

  it("maps known keys for Flink 1.20", () => {
    const config = {
      "state.backend.type": "rocksdb",
      "pipeline.operator-chaining.enabled": "true",
    }

    const result = FlinkVersionCompat.normalizeConfig(config, "1.20")
    expect(result["state.backend"]).toBe("rocksdb")
    expect(result["pipeline.operator-chaining"]).toBe("true")
  })

  it("passes through unknown keys unchanged for 1.20", () => {
    const config = { "custom.key": "value" }
    const result = FlinkVersionCompat.normalizeConfig(config, "1.20")
    expect(result["custom.key"]).toBe("value")
  })
})

describe("FlinkVersionCompat.checkFeature", () => {
  it("returns null for supported features", () => {
    expect(FlinkVersionCompat.checkFeature("CREATE_MODEL", "2.1")).toBeNull()
    expect(FlinkVersionCompat.checkFeature("CREATE_MODEL", "2.2")).toBeNull()
    expect(
      FlinkVersionCompat.checkFeature("MATERIALIZED_TABLE", "2.0"),
    ).toBeNull()
  })

  it("returns error for unsupported features", () => {
    const error = FlinkVersionCompat.checkFeature("CREATE_MODEL", "1.20")
    expect(error).not.toBeNull()
    expect(error?.feature).toBe("CREATE_MODEL")
    expect(error?.requiredVersion).toBe("2.1")
    expect(error?.currentVersion).toBe("1.20")
    expect(error?.message).toContain("requires Flink 2.1 or later")
  })

  it("returns error for VECTOR_SEARCH on 2.1", () => {
    const error = FlinkVersionCompat.checkFeature("VECTOR_SEARCH", "2.1")
    expect(error).not.toBeNull()
    expect(error?.requiredVersion).toBe("2.2")
  })

  it("returns null for unknown features (assumed supported)", () => {
    expect(FlinkVersionCompat.checkFeature("UNKNOWN_THING", "1.20")).toBeNull()
  })

  it("returns null for QUALIFY on Flink 2.0+", () => {
    expect(FlinkVersionCompat.checkFeature("QUALIFY", "2.0")).toBeNull()
    expect(FlinkVersionCompat.checkFeature("QUALIFY", "2.1")).toBeNull()
    expect(FlinkVersionCompat.checkFeature("QUALIFY", "2.2")).toBeNull()
  })

  it("returns error for QUALIFY on Flink 1.20", () => {
    const error = FlinkVersionCompat.checkFeature("QUALIFY", "1.20")
    expect(error).not.toBeNull()
    expect(error?.feature).toBe("QUALIFY")
    expect(error?.requiredVersion).toBe("2.0")
    expect(error?.currentVersion).toBe("1.20")
    expect(error?.message).toContain("requires Flink 2.0 or later")
  })
})

describe("FlinkVersionCompat.resolveJdbcConnector", () => {
  it("returns single JAR for Flink 1.20", () => {
    const result = FlinkVersionCompat.resolveJdbcConnector("1.20", "mysql")
    expect(result.style).toBe("single")
    expect(result.jars).toHaveLength(1)
    expect(result.jars[0]).toContain("1.20")
  })

  it("returns modular JARs for Flink 2.0+", () => {
    const result = FlinkVersionCompat.resolveJdbcConnector("2.0", "mysql")
    expect(result.style).toBe("modular")
    expect(result.jars).toHaveLength(2)
    expect(result.jars[0]).toContain("2.0")
    expect(result.jars[1]).toContain("mysql")
  })

  it("uses correct version in JAR names", () => {
    const result = FlinkVersionCompat.resolveJdbcConnector("2.2", "postgres")
    expect(result.jars[0]).toContain("2.2")
    expect(result.jars[1]).toContain("postgres")
    expect(result.jars[1]).toContain("2.2")
  })
})

describe("FlinkVersionCompat.isVersionAtLeast", () => {
  it("compares versions correctly", () => {
    expect(FlinkVersionCompat.isVersionAtLeast("2.0", "1.20")).toBe(true)
    expect(FlinkVersionCompat.isVersionAtLeast("1.20", "2.0")).toBe(false)
    expect(FlinkVersionCompat.isVersionAtLeast("2.2", "2.2")).toBe(true)
    expect(FlinkVersionCompat.isVersionAtLeast("2.1", "2.2")).toBe(false)
  })
})
