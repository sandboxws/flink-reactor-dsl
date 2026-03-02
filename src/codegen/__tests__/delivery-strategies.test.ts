import { beforeEach, describe, expect, it } from "vitest"
import type { ResolvedJar } from "@/codegen/connector-resolver.js"
import { resolveConnectors } from "@/codegen/connector-resolver.js"
import { generateCrd } from "@/codegen/crd-generator.js"
import {
  applyInitContainerPatch,
  applyMavenMirror,
  applyPrivateRegistry,
  generateDockerfile,
  generateInitContainerPatch,
} from "@/codegen/delivery-strategies.js"
import { Pipeline } from "@/components/pipeline.js"
import { KafkaSink } from "@/components/sinks.js"
import { KafkaSource } from "@/components/sources.js"
import { resetNodeIdCounter } from "@/core/jsx-runtime.js"
import { Field, Schema } from "@/core/schema.js"

beforeEach(() => {
  resetNodeIdCounter()
})

const OrderSchema = Schema({
  fields: {
    order_id: Field.BIGINT(),
    amount: Field.DECIMAL(10, 2),
    event_time: Field.TIMESTAMP(3),
  },
})

function makeTestJars(): ResolvedJar[] {
  return [
    {
      artifact: {
        groupId: "org.apache.flink",
        artifactId: "flink-sql-connector-kafka",
        version: "4.0.1-2.0",
      },
      jarName: "flink-sql-connector-kafka-4.0.1-2.0.jar",
      downloadUrl:
        "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.1-2.0/flink-sql-connector-kafka-4.0.1-2.0.jar",
      provenance: ["KafkaSource_0"],
    },
    {
      artifact: {
        groupId: "org.apache.flink",
        artifactId: "flink-sql-avro",
        version: "1.20.0",
      },
      jarName: "flink-sql-avro-1.20.0.jar",
      downloadUrl:
        "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-avro/1.20.0/flink-sql-avro-1.20.0.jar",
      provenance: ["KafkaSource_0"],
    },
  ]
}

// ── Init-Container Strategy ─────────────────────────────────────────

describe("init-container delivery", () => {
  it("generates correct initContainers YAML structure", () => {
    const jars = makeTestJars()
    const patch = generateInitContainerPatch(jars)

    expect(patch.initContainers).toHaveLength(1)
    expect(patch.initContainers[0].name).toBe("fetch-connectors")
    expect(patch.initContainers[0].image).toBe("busybox:1.36")

    // Command should have wget for each JAR
    const cmd = patch.initContainers[0].command
    expect(cmd[0]).toBe("sh")
    expect(cmd[1]).toBe("-c")
    expect(cmd[2]).toContain(
      "wget -O /opt/flink/usrlib/flink-sql-connector-kafka-4.0.1-2.0.jar",
    )
    expect(cmd[2]).toContain(
      "wget -O /opt/flink/usrlib/flink-sql-avro-1.20.0.jar",
    )
    expect(cmd[2]).toContain(" && ")
  })

  it("creates shared emptyDir volume at /opt/flink/usrlib/", () => {
    const jars = makeTestJars()
    const patch = generateInitContainerPatch(jars)

    expect(patch.volumes).toHaveLength(1)
    expect(patch.volumes[0].name).toBe("flink-usrlib")
    expect(patch.volumes[0].emptyDir).toEqual({})

    expect(patch.volumeMounts).toHaveLength(1)
    expect(patch.volumeMounts[0].mountPath).toBe("/opt/flink/usrlib")
  })

  it("generates pipeline.classpaths entries", () => {
    const jars = makeTestJars()
    const patch = generateInitContainerPatch(jars)

    expect(patch.classpaths).toHaveLength(2)
    expect(patch.classpaths[0]).toBe(
      "file:///opt/flink/usrlib/flink-sql-connector-kafka-4.0.1-2.0.jar",
    )
    expect(patch.classpaths[1]).toBe(
      "file:///opt/flink/usrlib/flink-sql-avro-1.20.0.jar",
    )
  })

  it("returns empty patch for zero JARs", () => {
    const patch = generateInitContainerPatch([])
    expect(patch.initContainers).toHaveLength(0)
    expect(patch.volumes).toHaveLength(0)
    expect(patch.classpaths).toHaveLength(0)
  })

  it("applies patch to CRD object", () => {
    const pipeline = Pipeline({
      name: "test",
      parallelism: 2,
      children: [],
    })

    const crd = generateCrd(pipeline, { flinkVersion: "2.0" })
    const jars = makeTestJars()
    const patch = generateInitContainerPatch(jars)
    const patched = applyInitContainerPatch(crd, patch)

    // flinkConfiguration should have pipeline.classpaths
    expect(patched.spec.flinkConfiguration["pipeline.classpaths"]).toContain(
      "file:///opt/flink/usrlib/flink-sql-connector-kafka-4.0.1-2.0.jar",
    )

    // podTemplate should exist with initContainers
    const podTemplate = patched.spec.podTemplate as Record<string, unknown>
    expect(podTemplate).toBeDefined()
    const spec = podTemplate.spec as Record<string, unknown>
    expect(spec.initContainers).toHaveLength(1)
    expect(spec.volumes).toHaveLength(1)
  })
})

// ── Custom-Image Strategy ───────────────────────────────────────────

describe("custom-image delivery", () => {
  it("generates valid Dockerfile", () => {
    const jars = makeTestJars()
    const result = generateDockerfile(jars, "flink:2.0")

    expect(result.dockerfile).toContain("FROM flink:2.0")
    expect(result.dockerfile).toContain(
      "ADD https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.1-2.0/flink-sql-connector-kafka-4.0.1-2.0.jar /opt/flink/usrlib/flink-sql-connector-kafka-4.0.1-2.0.jar",
    )
    expect(result.dockerfile).toContain(
      "ADD https://repo1.maven.org/maven2/org/apache/flink/flink-sql-avro/1.20.0/flink-sql-avro-1.20.0.jar /opt/flink/usrlib/flink-sql-avro-1.20.0.jar",
    )
    expect(result.dockerfile).toContain(
      "RUN chmod -R 644 /opt/flink/usrlib/*.jar",
    )
  })

  it("generates minimal Dockerfile for zero JARs", () => {
    const result = generateDockerfile([], "flink:2.0")
    expect(result.dockerfile).toContain("FROM flink:2.0")
    expect(result.dockerfile).not.toContain("ADD")
  })
})

// ── Air-gapped: Maven Mirror ────────────────────────────────────────

describe("air-gapped: Maven mirror substitution", () => {
  it("substitutes Maven Central URL with mirror", () => {
    const jars = makeTestJars()
    const mirrored = applyMavenMirror(
      jars,
      "https://nexus.internal/maven-central",
    )

    for (const jar of mirrored) {
      expect(jar.downloadUrl).toContain("https://nexus.internal/maven-central")
      expect(jar.downloadUrl).not.toContain("repo1.maven.org")
    }

    // Jar name and artifact should be unchanged
    expect(mirrored[0].jarName).toBe("flink-sql-connector-kafka-4.0.1-2.0.jar")
    expect(mirrored[0].artifact.artifactId).toBe("flink-sql-connector-kafka")
  })
})

// ── Air-gapped: Private Registry ────────────────────────────────────

describe("air-gapped: private registry", () => {
  it("prefixes image with registry", () => {
    const pipeline = Pipeline({ name: "test", children: [] })
    const crd = generateCrd(pipeline, { flinkVersion: "2.0" })
    const patched = applyPrivateRegistry(crd, "registry.internal.com")

    expect(patched.spec.image).toBe("registry.internal.com/flink:2.0")
  })

  it("adds imagePullSecrets when provided", () => {
    const pipeline = Pipeline({ name: "test", children: [] })
    const crd = generateCrd(pipeline, { flinkVersion: "2.0" })
    const patched = applyPrivateRegistry(
      crd,
      "registry.internal.com",
      "my-pull-secret",
    )

    expect(patched.spec.image).toBe("registry.internal.com/flink:2.0")
    const podTemplate = patched.spec.podTemplate as Record<string, unknown>
    const spec = podTemplate.spec as Record<string, unknown>
    expect(spec.imagePullSecrets).toEqual([{ name: "my-pull-secret" }])
  })
})

// ── End-to-end: resolve + init-container ────────────────────────────

describe("end-to-end: resolve + init-container delivery", () => {
  it("resolves connectors and applies init-container patch", () => {
    const source = KafkaSource({
      topic: "orders",
      format: "avro",
      schema: OrderSchema,
    })

    const sink = KafkaSink({
      topic: "output",
      children: [source],
    })

    const pipeline = Pipeline({
      name: "e2e-test",
      parallelism: 4,
      children: [sink],
    })

    // Resolve connectors
    const resolved = resolveConnectors(pipeline, { flinkVersion: "2.0" })

    // Generate CRD
    const crd = generateCrd(pipeline, { flinkVersion: "2.0" })

    // Apply delivery
    const patch = generateInitContainerPatch(resolved.jars)
    const finalCrd = applyInitContainerPatch(crd, patch)

    // Verify complete result
    expect(finalCrd.metadata.name).toBe("e2e-test")
    expect(finalCrd.spec.job.parallelism).toBe(4)
    expect(
      finalCrd.spec.flinkConfiguration["pipeline.classpaths"],
    ).toBeDefined()

    const podTemplate = finalCrd.spec.podTemplate as Record<string, unknown>
    const spec = podTemplate.spec as Record<string, unknown>
    expect((spec.initContainers as unknown[]).length).toBeGreaterThan(0)
  })
})
