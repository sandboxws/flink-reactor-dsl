// Shape tests for the bundled observability files. These are not deep
// behavioural assertions — Grafana itself validates the JSON at provision
// time. The goal here is to catch silent drift: if a contributor renames
// the `prometheus` datasource UID, swaps Gruvppuccin hexes for stock
// Grafana colors, or rearranges the directory structure that
// docker-compose's bind mounts expect, we fail at unit-test time rather
// than mid-debug at runtime.

import { readFileSync } from "node:fs"
import { join } from "node:path"
import { describe, expect, it } from "vitest"

const observabilityDir = join(__dirname, "..", "observability")

const dashboardsDir = join(observabilityDir, "grafana", "dashboards")
const themeDir = join(observabilityDir, "grafana", "theme")
const provisioningDir = join(observabilityDir, "grafana", "provisioning")

// Gruvppuccin tokens we expect to see baked into panel field configs.
// Sourced from /Users/ahmed/Development/github/gruvpuccin/gruvppuccin.
const GRUVPPUCCIN_TOKENS = [
  "#a9b665", // green — running / primary
  "#e78a4e", // peach — secondary action
  "#7daea3", // teal — info / completed
  "#ea6962", // red — failed
  "#d8a657", // yellow — warning / cancelled
] as const

describe("observability dashboards", () => {
  it("flink-overview.json parses, has the expected uid + datasource", () => {
    const raw = readFileSync(join(dashboardsDir, "flink-overview.json"), "utf8")
    const dashboard = JSON.parse(raw) as {
      uid: string
      title: string
      panels: Array<{ datasource?: { uid?: string } }>
    }

    expect(dashboard.uid).toBe("flink-overview")
    expect(dashboard.title).toBe("Flink Cluster Overview")
    expect(dashboard.panels.length).toBeGreaterThan(0)

    // Every panel must reference the auto-provisioned `prometheus`
    // datasource. A typo here breaks the whole dashboard at provision time
    // — Grafana logs the error and falls back to "no data".
    for (const panel of dashboard.panels) {
      expect(panel.datasource?.uid).toBe("prometheus")
    }
  })

  it("pg-fluss-paimon.json parses, has the expected uid + job_name variable", () => {
    const raw = readFileSync(
      join(dashboardsDir, "pg-fluss-paimon.json"),
      "utf8",
    )
    const dashboard = JSON.parse(raw) as {
      uid: string
      title: string
      panels: Array<{ datasource?: { uid?: string } }>
      templating: { list: Array<{ name: string; type: string }> }
    }

    expect(dashboard.uid).toBe("pg-fluss-paimon")
    expect(dashboard.title).toBe("pg-fluss-paimon Pipeline")
    for (const panel of dashboard.panels) {
      expect(panel.datasource?.uid).toBe("prometheus")
    }

    const jobVar = dashboard.templating.list.find((v) => v.name === "job_name")
    expect(jobVar?.type).toBe("query")
  })

  it("dashboards use Gruvppuccin colors (not stock Grafana defaults)", () => {
    for (const file of ["flink-overview.json", "pg-fluss-paimon.json"]) {
      const raw = readFileSync(join(dashboardsDir, file), "utf8")
      const tokensFound = GRUVPPUCCIN_TOKENS.filter((hex) => raw.includes(hex))

      // Don't require all tokens — different dashboards use different
      // subsets — but every dashboard should hit at least three to ensure
      // the theme isn't accidentally lost.
      expect(
        tokensFound.length,
        `${file} should reference at least 3 Gruvppuccin colors, found ${tokensFound.length} (${tokensFound.join(", ")})`,
      ).toBeGreaterThanOrEqual(3)
    }
  })
})

describe("observability provisioning", () => {
  it("ships a Prometheus datasource provisioning file", () => {
    const raw = readFileSync(
      join(provisioningDir, "datasources", "prometheus.yml"),
      "utf8",
    )
    expect(raw).toContain("name: prometheus")
    expect(raw).toContain("type: prometheus")
    expect(raw).toContain("http://prometheus:9090")
    expect(raw).toContain("isDefault: true")
  })

  it("ships a dashboard provider config pointing at /var/lib/grafana/dashboards", () => {
    const raw = readFileSync(
      join(provisioningDir, "dashboards", "flink-reactor.yml"),
      "utf8",
    )
    expect(raw).toContain("path: /var/lib/grafana/dashboards")
    expect(raw).toContain("disableDeletion: true")
  })

  it("Prometheus scrape config targets all 3 Flink containers on port 9249", () => {
    const raw = readFileSync(
      join(observabilityDir, "prometheus", "prometheus.yml"),
      "utf8",
    )
    expect(raw).toContain("jobmanager:9249")
    expect(raw).toContain("taskmanager-1:9249")
    expect(raw).toContain("taskmanager-2:9249")
  })
})

describe("observability theme", () => {
  it("gruvppuccin.css overrides the Grafana background CSS variables", () => {
    const raw = readFileSync(join(themeDir, "gruvppuccin.css"), "utf8")
    // The variable assignments are what win against Grafana's theme
    // defaults at runtime. Each must reference a Gruvppuccin token.
    expect(raw).toContain("--background-canvas:")
    expect(raw).toContain("--background-primary:")
    expect(raw).toContain("--text-primary:")
    expect(raw).toContain("--gp-green: #a9b665")
    expect(raw).toContain("--gp-peach: #e78a4e")
    expect(raw).toContain("--gp-text: #d4be98")
  })
})

describe("npm packaging", () => {
  it("package.json#files includes the observability tree", () => {
    const raw = readFileSync(
      join(__dirname, "..", "..", "..", "..", "package.json"),
      "utf8",
    )
    const pkg = JSON.parse(raw) as { files: readonly string[] }
    const observabilityPattern = pkg.files.find((p) =>
      p.includes("observability"),
    )
    expect(
      observabilityPattern,
      "package.json#files must include observability/**/* so dashboards, CSS, and prometheus.yml ship in the published tarball",
    ).toBeDefined()
  })
})
