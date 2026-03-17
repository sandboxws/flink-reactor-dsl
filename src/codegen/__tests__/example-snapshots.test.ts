import { describe, expect, it } from "vitest"
import { resetNodeIdCounter } from "@/core/jsx-runtime.js"
import { synth } from "@/testing/synth.js"

const EXAMPLES = [
  "01-simple-source-sink",
  "02-filter-project",
  "03-group-aggregate",
  "04-tumble-window",
  "05-hop-window",
  "06-interval-join",
  "07-lookup-join",
  "08-multi-stream-join",
  "11-dedup-window",
  "12-session-window",
  "13-simple-etl",
  "14-ohlcv-window",
  "15-cep-fraud-detection",
  "16-temporal-join",
  "18-broadcast-join",
  "19-union-aggregate",
  "20-realtime-dashboard",
  "21-branching-multi-sink",
  "22-enrichment-archive",
  "23-batch-etl",
  "24-lambda-architecture",
  "25-batch-reporting",
  "26-cdc-sync",
  "27-ml-feature-pipeline",
  "28-branching-iot",
  // Sandbox-derived examples
  "30-flatmap-unnest",
  "31-top-n-ranking",
  "32-union-streams",
  "33-rename-fields",
  "34-drop-fields",
  "35-cast-types",
  "36-coalesce-defaults",
  "37-add-computed-field",
  "38-dedup-aggregate",
]

describe("Example SQL Snapshots", () => {
  for (const id of EXAMPLES) {
    it(id, async () => {
      resetNodeIdCounter()
      const mod = await import(`../../examples/${id}/after.tsx`)
      const { sql } = synth(mod.default)
      expect(sql).toMatchSnapshot()
    })
  }
})
