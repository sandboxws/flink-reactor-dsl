import { describe, expect, it } from "vitest"
import { createRulesRegistry } from "../src/component-rules"

describe("createRulesRegistry", () => {
  const registry = createRulesRegistry()

  describe("getAllowedChildren", () => {
    it("returns the list for Pipeline", () => {
      const allowed = registry.getAllowedChildren("Pipeline")
      expect(Array.isArray(allowed)).toBe(true)
      expect(allowed).toContain("KafkaSource")
      expect(allowed).toContain("Filter")
      expect(allowed).toContain("Route")
      expect(allowed).toContain("KafkaSink")
    })

    it("returns explicit list for Route", () => {
      expect(registry.getAllowedChildren("Route")).toEqual([
        "Route.Branch",
        "Route.Default",
      ])
    })

    it("returns wildcard for Route.Branch", () => {
      expect(registry.getAllowedChildren("Route.Branch")).toBe("*")
    })

    it("returns wildcard for Route.Default", () => {
      expect(registry.getAllowedChildren("Route.Default")).toBe("*")
    })

    it("returns list for Query", () => {
      const allowed = registry.getAllowedChildren("Query")
      expect(allowed).toContain("Query.Select")
      expect(allowed).toContain("Query.Where")
      expect(allowed).toContain("KafkaSource")
    })

    it("returns undefined for unknown parent", () => {
      expect(registry.getAllowedChildren("UnknownComponent")).toBeUndefined()
    })
  })

  describe("isValidChild", () => {
    it("accepts valid children of Pipeline", () => {
      expect(registry.isValidChild("Pipeline", "KafkaSource")).toBe(true)
      expect(registry.isValidChild("Pipeline", "Filter")).toBe(true)
    })

    it("rejects invalid children of Pipeline", () => {
      expect(registry.isValidChild("Pipeline", "Route.Branch")).toBe(false)
    })

    it("accepts only Route.Branch and Route.Default inside Route", () => {
      expect(registry.isValidChild("Route", "Route.Branch")).toBe(true)
      expect(registry.isValidChild("Route", "Route.Default")).toBe(true)
      expect(registry.isValidChild("Route", "Filter")).toBe(false)
      expect(registry.isValidChild("Route", "KafkaSink")).toBe(false)
    })

    it("accepts any child for wildcard parents", () => {
      expect(registry.isValidChild("Route.Branch", "Filter")).toBe(true)
      expect(registry.isValidChild("Route.Branch", "KafkaSink")).toBe(true)
      expect(registry.isValidChild("Route.Branch", "Anything")).toBe(true)
    })

    it("accepts any child for unknown parents (passthrough)", () => {
      expect(registry.isValidChild("MyCustomComponent", "Filter")).toBe(true)
    })
  })

  describe("user config override", () => {
    it("merges user rules with built-in rules", () => {
      const custom = createRulesRegistry({
        MyContainer: ["Filter", "Map"],
      })
      expect(custom.getAllowedChildren("MyContainer")).toEqual([
        "Filter",
        "Map",
      ])
      // Built-in rules still work
      expect(custom.getAllowedChildren("Pipeline")).toContain("KafkaSource")
    })

    it("user rules override built-in rules", () => {
      const custom = createRulesRegistry({
        Route: ["Route.Branch", "Route.Default", "Filter"],
      })
      expect(custom.isValidChild("Route", "Filter")).toBe(true)
    })

    it("user can set wildcard for any parent", () => {
      const custom = createRulesRegistry({
        Pipeline: "*",
      })
      expect(custom.getAllowedChildren("Pipeline")).toBe("*")
      expect(custom.isValidChild("Pipeline", "Anything")).toBe(true)
    })
  })
})
