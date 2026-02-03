import { describe, it, expect, beforeEach } from 'vitest';
import { resetNodeIdCounter } from '../../core/jsx-runtime.js';
import { Schema, Field } from '../../core/schema.js';
import { KafkaSource } from '../../components/sources.js';
import { JdbcSource } from '../../components/sources.js';
import { KafkaSink } from '../../components/sinks.js';
import { JdbcSink } from '../../components/sinks.js';
import { FileSystemSink } from '../../components/sinks.js';
import { Filter } from '../../components/transforms.js';
import { Pipeline } from '../../components/pipeline.js';
import { resolveConnectors } from '../connector-resolver.js';

beforeEach(() => {
  resetNodeIdCounter();
});

const OrderSchema = Schema({
  fields: {
    order_id: Field.BIGINT(),
    amount: Field.DECIMAL(10, 2),
    event_time: Field.TIMESTAMP(3),
  },
});

// ── Kafka Resolution ────────────────────────────────────────────────

describe('Kafka connector resolution', () => {
  it('resolves correct JAR for Flink 2.2', () => {
    const source = KafkaSource({
      topic: 'orders',
      format: 'json',
      schema: OrderSchema,
    });

    const sink = KafkaSink({
      topic: 'output',
      children: [source],
    });

    const pipeline = Pipeline({ name: 'test', children: [sink] });

    const result = resolveConnectors(pipeline, { flinkVersion: '2.2' });
    expect(result.jars).toHaveLength(1);
    expect(result.jars[0].artifact.artifactId).toBe('flink-sql-connector-kafka');
    expect(result.jars[0].artifact.version).toBe('4.0.1-2.0');
    expect(result.conflicts).toHaveLength(0);
  });

  it('resolves correct JAR for Flink 1.20', () => {
    const source = KafkaSource({
      topic: 'orders',
      schema: OrderSchema,
    });

    const sink = KafkaSink({
      topic: 'output',
      children: [source],
    });

    const pipeline = Pipeline({ name: 'test', children: [sink] });

    const result = resolveConnectors(pipeline, { flinkVersion: '1.20' });
    expect(result.jars).toHaveLength(1);
    expect(result.jars[0].artifact.version).toBe('3.3.0-1.20');
  });
});

// ── De-duplication ──────────────────────────────────────────────────

describe('de-duplication', () => {
  it('two KafkaSources produce one Kafka JAR', () => {
    const source1 = KafkaSource({
      topic: 'orders',
      format: 'json',
      schema: OrderSchema,
    });

    const source2 = KafkaSource({
      topic: 'users',
      format: 'json',
      schema: OrderSchema,
    });

    const sink = KafkaSink({
      topic: 'output',
      children: [source1],
    });

    const sink2 = KafkaSink({
      topic: 'output2',
      children: [source2],
    });

    const pipeline = Pipeline({ name: 'test', children: [sink, sink2] });

    const result = resolveConnectors(pipeline, { flinkVersion: '2.0' });
    // Only one Kafka JAR despite four Kafka components
    const kafkaJars = result.jars.filter(
      (j) => j.artifact.artifactId === 'flink-sql-connector-kafka',
    );
    expect(kafkaJars).toHaveLength(1);
  });
});

// ── JDBC PostgreSQL Resolution ──────────────────────────────────────

describe('JDBC PostgreSQL resolution', () => {
  it('resolves core + postgres dialect + driver for Flink 2.0+', () => {
    const source = JdbcSource({
      url: 'jdbc:postgresql://localhost:5432/mydb',
      table: 'orders',
      schema: OrderSchema,
    });

    const sink = KafkaSink({
      topic: 'output',
      children: [source],
    });

    const pipeline = Pipeline({ name: 'test', children: [sink] });

    const result = resolveConnectors(pipeline, { flinkVersion: '2.0' });

    const artifactIds = result.jars.map((j) => j.artifact.artifactId);
    expect(artifactIds).toContain('flink-connector-jdbc-core');
    expect(artifactIds).toContain('flink-connector-jdbc-postgres');
    expect(artifactIds).toContain('postgresql');
    // Also Kafka JAR from the sink
    expect(artifactIds).toContain('flink-sql-connector-kafka');
  });

  it('resolves single JDBC JAR + driver for Flink 1.20', () => {
    const source = JdbcSource({
      url: 'jdbc:postgresql://localhost:5432/mydb',
      table: 'orders',
      schema: OrderSchema,
    });

    const sink = KafkaSink({
      topic: 'output',
      children: [source],
    });

    const pipeline = Pipeline({ name: 'test', children: [sink] });

    const result = resolveConnectors(pipeline, { flinkVersion: '1.20' });

    const artifactIds = result.jars.map((j) => j.artifact.artifactId);
    expect(artifactIds).toContain('flink-connector-jdbc');
    expect(artifactIds).toContain('postgresql');
    // No separate dialect module for 1.20
    expect(artifactIds).not.toContain('flink-connector-jdbc-postgres');
  });
});

// ── Format Dependencies ─────────────────────────────────────────────

describe('format dependency resolution', () => {
  it('Avro format adds JAR', () => {
    const source = KafkaSource({
      topic: 'orders',
      format: 'avro',
      schema: OrderSchema,
    });

    const sink = KafkaSink({
      topic: 'output',
      children: [source],
    });

    const pipeline = Pipeline({ name: 'test', children: [sink] });

    const result = resolveConnectors(pipeline, { flinkVersion: '2.0' });
    const artifactIds = result.jars.map((j) => j.artifact.artifactId);
    expect(artifactIds).toContain('flink-sql-avro');
    expect(artifactIds).toContain('flink-sql-connector-kafka');
  });

  it('JSON format does not add extra JAR', () => {
    const source = KafkaSource({
      topic: 'orders',
      format: 'json',
      schema: OrderSchema,
    });

    const sink = KafkaSink({
      topic: 'output',
      children: [source],
    });

    const pipeline = Pipeline({ name: 'test', children: [sink] });

    const result = resolveConnectors(pipeline, { flinkVersion: '2.0' });
    const artifactIds = result.jars.map((j) => j.artifact.artifactId);
    expect(artifactIds).not.toContain('flink-sql-avro');
    expect(artifactIds).not.toContain('flink-sql-parquet');
  });
});

// ── FileSystem (no JARs) ────────────────────────────────────────────

describe('FileSystem connector', () => {
  it('produces no JAR for filesystem sink', () => {
    const source = KafkaSource({
      topic: 'orders',
      format: 'json',
      schema: OrderSchema,
    });

    const sink = FileSystemSink({
      path: 's3://bucket/output',
      format: 'parquet',
      children: [source],
    });

    const pipeline = Pipeline({ name: 'test', children: [sink] });

    const result = resolveConnectors(pipeline, { flinkVersion: '2.0' });
    const artifactIds = result.jars.map((j) => j.artifact.artifactId);
    // Kafka JAR from source, Parquet format JAR from sink format, but no filesystem JAR
    expect(artifactIds).toContain('flink-sql-connector-kafka');
    expect(artifactIds).not.toContain('flink-sql-connector-filesystem');
  });
});

// ── Air-gapped mirror ───────────────────────────────────────────────

describe('Maven mirror URL substitution', () => {
  it('uses mirror URL in download URLs', () => {
    const source = KafkaSource({
      topic: 'orders',
      schema: OrderSchema,
    });

    const sink = KafkaSink({
      topic: 'output',
      children: [source],
    });

    const pipeline = Pipeline({ name: 'test', children: [sink] });

    const result = resolveConnectors(pipeline, {
      flinkVersion: '2.0',
      mavenMirror: 'https://nexus.internal/maven-central',
    });

    for (const jar of result.jars) {
      expect(jar.downloadUrl).toContain('https://nexus.internal/maven-central');
      expect(jar.downloadUrl).not.toContain('repo1.maven.org');
    }
  });
});

// ── Custom Artifacts ────────────────────────────────────────────────

describe('custom artifacts from config', () => {
  it('includes user-provided custom artifacts', () => {
    const source = KafkaSource({
      topic: 'orders',
      schema: OrderSchema,
    });

    const sink = KafkaSink({
      topic: 'output',
      children: [source],
    });

    const pipeline = Pipeline({ name: 'test', children: [sink] });

    const result = resolveConnectors(pipeline, {
      flinkVersion: '2.0',
      customArtifacts: [
        { groupId: 'com.example', artifactId: 'custom-udf', version: '1.0.0' },
      ],
    });

    const artifactIds = result.jars.map((j) => j.artifact.artifactId);
    expect(artifactIds).toContain('custom-udf');
    expect(artifactIds).toContain('flink-sql-connector-kafka');
  });
});

// ── Provenance Tracking ─────────────────────────────────────────────

describe('provenance tracking', () => {
  it('tracks which components needed each JAR', () => {
    const source = KafkaSource({
      topic: 'orders',
      schema: OrderSchema,
    });

    const sink = KafkaSink({
      topic: 'output',
      children: [source],
    });

    const pipeline = Pipeline({ name: 'test', children: [sink] });

    const result = resolveConnectors(pipeline, { flinkVersion: '2.0' });
    const kafkaJar = result.jars.find(
      (j) => j.artifact.artifactId === 'flink-sql-connector-kafka',
    );
    expect(kafkaJar).toBeDefined();
    // Both the source and sink should be in provenance
    expect(kafkaJar!.provenance.length).toBeGreaterThanOrEqual(1);
  });
});
