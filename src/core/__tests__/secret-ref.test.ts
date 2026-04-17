import { describe, expect, it } from "vitest"
import {
  deriveEnvName,
  isSecretRef,
  renderSecretPlaceholder,
  secretRef,
} from "@/core/secret-ref.js"

describe("deriveEnvName", () => {
  it("uppercases the Secret name and replaces dashes with underscores", () => {
    expect(deriveEnvName("pg-primary-password", "password")).toBe(
      "PG_PRIMARY_PASSWORD",
    )
  })

  it("appends uppercase key when key is not 'password'", () => {
    expect(deriveEnvName("db-auth", "readonly-user-password")).toBe(
      "DB_AUTH_READONLY_USER_PASSWORD",
    )
  })

  it("collapses runs of underscores", () => {
    expect(deriveEnvName("foo--bar", "password")).toBe("FOO_BAR")
  })

  it("strips invalid identifier characters", () => {
    expect(deriveEnvName("my.secret/v1", "password")).toBe("MY_SECRET_V1")
  })
})

describe("secretRef", () => {
  it("defaults the key to 'password'", () => {
    const ref = secretRef("pg-primary-password")
    expect(ref.name).toBe("pg-primary-password")
    expect(ref.key).toBe("password")
    expect(ref.envName).toBe("PG_PRIMARY_PASSWORD")
  })

  it("accepts an explicit key", () => {
    const ref = secretRef("db-auth", "readonly-user-password")
    expect(ref.key).toBe("readonly-user-password")
    expect(ref.envName).toBe("DB_AUTH_READONLY_USER_PASSWORD")
  })

  it("accepts an explicit envName override", () => {
    const ref = secretRef("foo", "password", "CUSTOM_VAR")
    expect(ref.envName).toBe("CUSTOM_VAR")
  })

  it("freezes the result", () => {
    const ref = secretRef("pg-primary-password")
    expect(Object.isFrozen(ref)).toBe(true)
  })

  it("throws on empty name", () => {
    expect(() => secretRef("")).toThrow(/non-empty Secret name/)
  })

  it("throws on empty key", () => {
    expect(() => secretRef("foo", "")).toThrow(/non-empty key/)
  })

  it("throws on an invalid envName override", () => {
    expect(() => secretRef("foo", "password", "lowercase-bad")).toThrow(
      /not a valid env-var identifier/,
    )
  })
})

describe("isSecretRef", () => {
  it("identifies a SecretRef produced by secretRef()", () => {
    expect(isSecretRef(secretRef("pg-primary-password"))).toBe(true)
  })

  it("rejects plain strings", () => {
    expect(isSecretRef("hunter2")).toBe(false)
  })

  it("rejects plain objects with matching shape", () => {
    expect(
      isSecretRef({
        name: "pg-primary-password",
        key: "password",
        envName: "PG_PRIMARY_PASSWORD",
      }),
    ).toBe(false)
  })

  it("rejects null and undefined", () => {
    expect(isSecretRef(null)).toBe(false)
    expect(isSecretRef(undefined)).toBe(false)
  })
})

describe("renderSecretPlaceholder", () => {
  it("renders the Flink CDC env-substitution syntax", () => {
    const ref = secretRef("pg-primary-password")
    expect(renderSecretPlaceholder(ref)).toBe("${env:PG_PRIMARY_PASSWORD}")
  })

  it("honours explicit envName", () => {
    const ref = secretRef("foo", "password", "CUSTOM_VAR")
    expect(renderSecretPlaceholder(ref)).toBe("${env:CUSTOM_VAR}")
  })
})
