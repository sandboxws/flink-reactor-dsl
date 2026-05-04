import type { ScaffoldOptions, TemplateFile } from "@/cli/commands/new.js"
import {
  pipelineReadme,
  sharedFiles,
  templatePipelineTestStub,
  templateReadme,
} from "./shared.js"

export function getIotFactoryTemplates(opts: ScaffoldOptions): TemplateFile[] {
  return [
    ...sharedFiles(opts),
    {
      path: "flink-reactor.config.ts",
      content: `import { defineConfig } from '@flink-reactor/dsl';

export default defineConfig({
  flink: { version: '${opts.flinkVersion}' },

  // Kafka for telemetry; Postgres for the JDBC sinks the pipelines write to.
  services: { kafka: { bootstrapServers: 'kafka:9092' }, postgres: {} },

  environments: {
    minikube: {
      cluster: { url: 'http://localhost:8081' },
      sim: {
        init: {
          kafka: {
            topics: ['iot.maintenance-alerts'],
            catalogs: [{
              name: 'iot',
              tables: [
                {
                  table: 'sensor_readings',
                  topic: 'iot.sensor-readings',
                  format: 'json',
                  columns: {
                    deviceId: 'STRING',
                    sensorType: 'STRING',
                    value: 'DOUBLE',
                    unit: 'STRING',
                    readingTime: 'TIMESTAMP(3)',
                  },
                  watermark: { column: 'readingTime', expression: "readingTime - INTERVAL '5' SECOND" },
                },
                {
                  table: 'device_registry',
                  topic: 'iot.device-registry',
                  format: 'debezium-json',
                  columns: {
                    deviceId: 'STRING',
                    location: 'STRING',
                    firmwareVersion: 'STRING',
                    installDate: 'STRING',
                    thresholdTemp: 'DOUBLE',
                    thresholdVibration: 'DOUBLE',
                    updateTime: 'TIMESTAMP(3)',
                  },
                  primaryKey: ['deviceId'],
                },
              ],
            }],
          },
          jdbc: {
            catalogs: [{
              name: 'flink_sink',
              baseUrl: 'jdbc:postgresql://postgres:5432/',
              defaultDatabase: 'flink_sink',
            }],
          },
        },
      },
      pipelines: { '*': { parallelism: 2 } },
    },
    production: {
      cluster: { url: 'https://flink-prod:8081' },
      kubernetes: { namespace: 'flink-prod' },
      pipelines: { '*': { parallelism: 4 } },
    },
  },
});
`,
    },
    {
      path: "schemas/iot.ts",
      content: `import { Schema, Field } from '@flink-reactor/dsl';

export const SensorReadingSchema = Schema({
  fields: {
    deviceId: Field.STRING(),
    sensorType: Field.STRING(),
    value: Field.DOUBLE(),
    unit: Field.STRING(),
    readingTime: Field.TIMESTAMP(3),
  },
  watermark: { column: 'readingTime', expression: "readingTime - INTERVAL '5' SECOND" },
});

export const DeviceRegistrySchema = Schema({
  fields: {
    deviceId: Field.STRING(),
    location: Field.STRING(),
    firmwareVersion: Field.STRING(),
    installDate: Field.STRING(),
    thresholdTemp: Field.DOUBLE(),
    thresholdVibration: Field.DOUBLE(),
    updateTime: Field.TIMESTAMP(3),
  },
  primaryKey: { columns: ['deviceId'] },
});
`,
    },
    {
      path: "pipelines/iot-predictive-maintenance/index.tsx",
      content: `import {
  Pipeline, KafkaSource, JdbcSource, KafkaSink, JdbcSink,
  SlideWindow, Aggregate, LookupJoin, Route,
} from '@flink-reactor/dsl';
import { SensorReadingSchema, DeviceRegistrySchema } from '@/schemas/iot';

const readings = KafkaSource({
  topic: "iot.sensor-readings",
  schema: SensorReadingSchema,
  bootstrapServers: "kafka:9092",
  consumerGroup: "iot-maintenance",
});

// Device registry is a slow-changing dimension — enrich via JDBC lookup
// from the aggregated stats. A Kafka-CDC + event-time TemporalJoin over
// a windowed aggregate hits a Flink-version quirk where window_end/
// window_start lose their rowtime-attribute through the subquery
// boundary (BUG-030); LookupJoin sidesteps it by running on a synthetic
// proctime, which is the natural pattern for dim-enrichment anyway.
const devices = JdbcSource({
  table: "device_registry",
  url: "jdbc:postgresql://postgres:5432/flink_sink",
  schema: DeviceRegistrySchema,
  lookupCache: { type: "lru", maxRows: 10000, ttl: "10min" },
});

const windowed = SlideWindow({ size: "5 MINUTE", slide: "30 SECOND", on: "readingTime", children: readings });

const stats = Aggregate({
  groupBy: ['deviceId', 'sensorType'],
  select: {
    avgValue: 'AVG(\`value\`)',
    stddevValue: 'STDDEV_POP(\`value\`)',
    maxValue: 'MAX(\`value\`)',
    readingCount: 'COUNT(*)',
  },
  children: windowed,
});

const enriched = LookupJoin({
  input: stats,
  table: "device_registry",
  url: "jdbc:postgresql://postgres:5432/flink_sink",
  on: "deviceId = deviceId",
});

export default (
  <Pipeline
    name="iot-predictive-maintenance"
    mode="streaming"
    parallelism={4}
    stateBackend="rocksdb"
    checkpoint={{ interval: "30s", mode: "exactly-once" }}
    flinkConfig={{
      "state.checkpoints.dir": "s3://flink-state/checkpoints/iot-predictive-maintenance",
      "state.savepoints.dir": "s3://flink-state/savepoints/iot-predictive-maintenance",
      "s3.endpoint": "http://seaweedfs.flink-demo.svc:8333",
      "s3.path.style.access": "true",
    }}
  >
    {readings}
    {devices}
    {enriched}
    <Route>
      <Route.Branch condition="stddevValue > thresholdTemp">
        <KafkaSink topic="iot.maintenance-alerts" bootstrapServers="kafka:9092" />
      </Route.Branch>
      <Route.Default>
        <JdbcSink table="sensor_dashboard" url="jdbc:postgresql://postgres:5432/flink_sink" />
      </Route.Default>
    </Route>
  </Pipeline>
);
`,
    },
    {
      path: "pipelines/pump-iot/index.tsx",
      content: `import {
  Pipeline, DataGenSource, KafkaSink, StatementSet,
} from '@flink-reactor/dsl';
import { SensorReadingSchema, DeviceRegistrySchema } from '@/schemas/iot';

export default (
  <Pipeline
    name="pump-iot"
    mode="streaming"
    parallelism={2}
    stateBackend="rocksdb"
    checkpoint={{ interval: "60s", mode: "exactly-once" }}
    flinkConfig={{
      "state.checkpoints.dir": "s3://flink-state/checkpoints/pump-iot",
      "state.savepoints.dir": "s3://flink-state/savepoints/pump-iot",
      "s3.endpoint": "http://seaweedfs.flink-demo.svc:8333",
      "s3.path.style.access": "true",
    }}
  >
    <StatementSet>
      <DataGenSource schema={SensorReadingSchema} rowsPerSecond={5000} />
      <KafkaSink topic="iot.sensor-readings" bootstrapServers="kafka:9092" />

      <DataGenSource schema={DeviceRegistrySchema} rowsPerSecond={20} />
      <KafkaSink topic="iot.device-registry" bootstrapServers="kafka:9092" />
    </StatementSet>
  </Pipeline>
);
`,
    },

    // ── Per-pipeline READMEs ──────────────────────────────────────────

    pipelineReadme({
      pipelineName: "iot-predictive-maintenance",
      tagline:
        "5-minute hopping-window per-device sensor stats with JDBC dimension lookup, routed to alerts vs dashboard sinks.",
      demonstrates: [
        '`<SlideWindow size="5 MINUTE" slide="30 SECOND" on="readingTime">` for overlapping sensor windows.',
        "`<Aggregate>` computing `AVG`, `STDDEV_POP`, `MAX`, and `COUNT(*)` per `(deviceId, sensorType)`.",
        "`<LookupJoin>` against a JDBC device-registry dimension with LRU cache (10k rows, 10min TTL) — the canonical pattern for slow-changing dimension enrichment without rowtime-attribute issues.",
        "`<Route>` forking high-stddev readings to `iot.maintenance-alerts` and the rest to `sensor_dashboard` JDBC.",
      ],
      topology: `KafkaSource (sensor-readings)
  └── SlideWindow (5min/30s, on=readingTime)
        └── Aggregate (GROUP BY deviceId, sensorType — AVG/STDDEV/MAX/COUNT)
              └── LookupJoin (devices, lru cache)
                    └── Route
                          ├── Branch (stddevValue > thresholdTemp) ─► KafkaSink (iot.maintenance-alerts)
                          └── Default ─► JdbcSink (sensor_dashboard)`,
      schemas: [
        "`schemas/iot.ts` — `SensorReadingSchema` (with `readingTime` watermark), `DeviceRegistrySchema` (with `deviceId` PK)",
      ],
      runCommand: `pnpm synth
pnpm test`,
    }),
    pipelineReadme({
      pipelineName: "pump-iot",
      tagline:
        "Internal data-generator pipeline that pumps synthetic sensor readings and device-registry records into the corresponding Kafka topics.",
      demonstrates: [
        "Two `<DataGenSource>` driving two `<KafkaSink>` inside a single `<StatementSet>`.",
        "Bundle-internal pump pattern (no upstream Apache Flink source — exists only to feed `iot-predictive-maintenance` on the local sim).",
      ],
      topology: `DataGenSource (SensorReading)   ─► KafkaSink (iot.sensor-readings)
DataGenSource (DeviceRegistry) ─► KafkaSink (iot.device-registry)`,
      schemas: ["`schemas/iot.ts` — same schemas the consumer reads"],
      runCommand: `pnpm synth
pnpm test`,
    }),

    // ── Tests ─────────────────────────────────────────────────────────

    templatePipelineTestStub({
      pipelineName: "iot-predictive-maintenance",
      loadBearingPatterns: [
        /HOP\(/i,
        /STDDEV_POP\(/i,
        /iot\.maintenance-alerts/,
      ],
    }),
    templatePipelineTestStub({
      pipelineName: "pump-iot",
      loadBearingPatterns: [/INSERT INTO/i, /datagen/i],
    }),

    // ── Project-root README ───────────────────────────────────────────

    templateReadme({
      templateName: "iot-factory",
      tagline:
        "IoT smart-factory predictive-maintenance pipeline plus an internal data pump. The main pipeline runs sliding-window per-device sensor stats, JDBC-lookup-joins against a device registry, and routes high-deviation readings to a maintenance-alerts topic.",
      pipelines: [
        {
          name: "iot-predictive-maintenance",
          pitch:
            "Sliding-window sensor stats × JDBC device-registry lookup, routed by stddev threshold to alerts or dashboard sinks.",
        },
        {
          name: "pump-iot",
          pitch:
            "Internal DataGen → Kafka pump for sensor-readings and device-registry topics.",
        },
      ],
      gettingStarted: ["pnpm install", "pnpm synth", "pnpm test"],
    }),
  ]
}
