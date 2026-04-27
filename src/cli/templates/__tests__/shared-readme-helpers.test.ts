import { describe, expect, it } from "vitest"
import {
  pipelineReadme,
  templatePipelineTestStub,
  templateReadme,
} from "@/cli/templates/shared.js"

describe("templateReadme", () => {
  it("returns a TemplateFile at README.md with title, tagline, and a section per pipeline", () => {
    const file = templateReadme({
      templateName: "foo-bar",
      tagline: "A short description.",
      pipelines: [
        { name: "p1", pitch: "demonstrates X" },
        { name: "p2", pitch: "demonstrates Y" },
      ],
    })

    expect(file.path).toBe("README.md")
    expect(file.content.length).toBeGreaterThan(0)
    expect(file.content).toContain("# Foo Bar")
    expect(file.content).toContain("A short description.")
    expect(file.content).toContain("**p1** — demonstrates X")
    expect(file.content).toContain("**p2** — demonstrates Y")
  })

  it("includes prerequisites and gettingStarted when supplied", () => {
    const file = templateReadme({
      templateName: "foo",
      tagline: "tag",
      pipelines: [{ name: "p1", pitch: "p1 pitch" }],
      prerequisites: ["Postgres on :5432"],
      gettingStarted: ["pnpm install", "pnpm dev"],
    })

    expect(file.content).toContain("## Prerequisites")
    expect(file.content).toContain("- Postgres on :5432")
    expect(file.content).toContain("## Getting Started")
    expect(file.content).toContain("pnpm install")
  })

  it("omits optional sections when not supplied", () => {
    const file = templateReadme({
      templateName: "foo",
      tagline: "tag",
      pipelines: [{ name: "p1", pitch: "x" }],
    })

    expect(file.content).not.toContain("## Prerequisites")
    expect(file.content).not.toContain("## Getting Started")
  })
})

describe("pipelineReadme", () => {
  const baseOpts = {
    pipelineName: "word-count",
    tagline: "Streaming word counts.",
    demonstrates: ["KafkaSource", "TumbleWindow"],
    topology: "Kafka → Window → Kafka",
    schemas: ["`schemas/words.ts`"],
    runCommand: "pnpm dev",
  }

  it("returns a TemplateFile at pipelines/<name>/README.md with all 7 required sections", () => {
    const file = pipelineReadme(baseOpts)

    expect(file.path).toBe("pipelines/word-count/README.md")
    expect(file.content.length).toBeGreaterThan(0)
    expect(file.content).toContain("# word-count")
    expect(file.content).toContain("Streaming word counts.")
    expect(file.content).toContain("## What it demonstrates")
    expect(file.content).toContain("- KafkaSource")
    expect(file.content).toContain("## Topology")
    expect(file.content).toContain("Kafka → Window → Kafka")
    expect(file.content).toContain("## Schemas")
    expect(file.content).toContain("## Run locally")
    expect(file.content).toContain("pnpm dev")
  })

  it("emits sections in the canonical order", () => {
    const file = pipelineReadme({
      ...baseOpts,
      source: "Adapted from WordCount.java",
      expectedOutput: "word=hello, count=1",
      translationNotes: "Used HOP instead of count windows",
    })

    const order = [
      "# word-count",
      "## Source",
      "## What it demonstrates",
      "## Topology",
      "## Schemas",
      "## Run locally",
      "## Expected Output",
      "## Translation Notes",
    ]
    let cursor = 0
    for (const heading of order) {
      const idx = file.content.indexOf(heading, cursor)
      expect(idx).toBeGreaterThan(cursor - 1)
      cursor = idx + heading.length
    }
  })

  it("includes the optional Translation Notes section when supplied", () => {
    const file = pipelineReadme({
      ...baseOpts,
      translationNotes: "Original used count-windows; we use HOP",
    })

    expect(file.content).toContain("## Translation Notes")
    expect(file.content).toContain("Original used count-windows; we use HOP")
  })

  it("omits Expected Output and Translation Notes when not supplied", () => {
    const file = pipelineReadme(baseOpts)

    expect(file.content).not.toContain("## Expected Output")
    expect(file.content).not.toContain("## Translation Notes")
  })

  it("omits the Source section when not supplied", () => {
    const file = pipelineReadme(baseOpts)
    expect(file.content).not.toContain("## Source")
  })
})

describe("templatePipelineTestStub", () => {
  it("returns a TemplateFile at tests/pipelines/<name>.test.ts with required scaffolding", () => {
    const file = templatePipelineTestStub({
      pipelineName: "word-count",
      loadBearingPatterns: [/GROUP BY/, /SUM\(/],
    })

    expect(file.path).toBe("tests/pipelines/word-count.test.ts")
    expect(file.content.length).toBeGreaterThan(0)
    expect(file.content).toContain("resetNodeIdCounter")
    expect(file.content).toContain("beforeEach")
    expect(file.content).toContain("synth(")
    expect(file.content).toContain("toMatchSnapshot()")
    expect(file.content).toContain("from '../../pipelines/word-count/index.js'")
  })

  it("emits one expect(sql).toMatch(...) per supplied pattern", () => {
    const file = templatePipelineTestStub({
      pipelineName: "p",
      loadBearingPatterns: [/GROUP BY/, /SUM\(/, /TUMBLE\(/],
    })

    expect(file.content).toContain("expect(sql).toMatch(/GROUP BY/)")
    expect(file.content).toContain("expect(sql).toMatch(/SUM\\(/)")
    expect(file.content).toContain("expect(sql).toMatch(/TUMBLE\\(/)")
  })

  it("emits no toMatch assertions when loadBearingPatterns is empty", () => {
    const file = templatePipelineTestStub({
      pipelineName: "p",
      loadBearingPatterns: [],
    })

    expect(file.content).not.toContain("expect(sql).toMatch(")
    expect(file.content).toContain("toMatchSnapshot()")
  })
})
