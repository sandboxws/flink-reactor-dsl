import { beforeEach, describe, expect, it } from "vitest"
import { resetNodeIdCounter } from "../../core/jsx-runtime.js"
import { SynthContext } from "../../core/synth-context.js"
import { CatalogSource } from "../catalog-source.js"
import { HiveCatalog, PaimonCatalog } from "../catalogs.js"
import { PaimonSink } from "../sinks.js"

beforeEach(() => {
  resetNodeIdCounter()
})

describe("CatalogSource", () => {
  it("creates a Source node referencing a catalog handle", () => {
    const { handle } = HiveCatalog({
      name: "hive",
      hiveConfDir: "/etc/hive/conf",
    })

    const node = CatalogSource({
      catalog: handle,
      database: "warehouse",
      table: "customers",
    })

    expect(node.kind).toBe("Source")
    expect(node.component).toBe("CatalogSource")
    expect(node.props.catalogName).toBe("hive")
    expect(node.props.catalogNodeId).toBe(handle.nodeId)
    expect(node.props.database).toBe("warehouse")
    expect(node.props.table).toBe("customers")
  })

  it("does not require a schema prop", () => {
    const { handle } = HiveCatalog({
      name: "hive",
      hiveConfDir: "/etc/hive/conf",
    })

    const node = CatalogSource({
      catalog: handle,
      database: "warehouse",
      table: "customers",
    })

    expect(node.props.schema).toBeUndefined()
  })
})

describe("Cross-catalog pipeline", () => {
  it("supports CatalogSource from Hive piped to PaimonSink", () => {
    const hive = HiveCatalog({
      name: "hive",
      hiveConfDir: "/etc/hive/conf",
    })

    const paimon = PaimonCatalog({
      name: "lake",
      warehouse: "s3://bucket/paimon",
    })

    const sinkNode = PaimonSink({
      catalog: paimon.handle,
      database: "analytics",
      table: "customers_v2",
      primaryKey: ["customer_id"],
      mergeEngine: "deduplicate",
    })

    const sourceNode = CatalogSource({
      catalog: hive.handle,
      database: "warehouse",
      table: "customers",
      children: sinkNode,
    })

    // Build graph and validate
    const ctx = new SynthContext()
    ctx.buildFromTree(sourceNode)

    expect(ctx.getAllNodes()).toHaveLength(2)
    const edges = ctx.getAllEdges()
    expect(edges).toHaveLength(1)
    expect(edges[0].from).toBe(sourceNode.id)
    expect(edges[0].to).toBe(sinkNode.id)

    // Source references Hive catalog
    expect(sourceNode.props.catalogName).toBe("hive")
    // Sink references Paimon catalog
    expect(sinkNode.props.catalogName).toBe("lake")
  })
})
