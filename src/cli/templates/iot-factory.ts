import type { ScaffoldOptions, TemplateFile } from "@/cli/commands/new.js"
import { sharedFiles } from "./shared.js"

export function getIotFactoryTemplates(opts: ScaffoldOptions): TemplateFile[] {
  return [
    ...sharedFiles(opts),
    {
      path: "flink-reactor.config.ts",
      content: `import { defineConfig } from '@flink-reactor/dsl';

export default defineConfig({
  flink: { version: '${opts.flinkVersion}' },

  environments: {
    minikube: {
      cluster: { url: 'http://localhost:8081' },
      kafka: { bootstrapServers: 'kafka:9092' },
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
  Pipeline, KafkaSource, KafkaSink, JdbcSink,
  SlideWindow, Aggregate, TemporalJoin, Route,
} from '@flink-reactor/dsl';
import { SensorReadingSchema, DeviceRegistrySchema } from '@/schemas/iot';

const readings = KafkaSource({
  topic: "iot.sensor-readings",
  schema: SensorReadingSchema,
  bootstrapServers: "kafka:9092",
  consumerGroup: "iot-maintenance",
});

const devices = KafkaSource({
  topic: "iot.device-registry",
  schema: DeviceRegistrySchema,
  format: "debezium-json",
  bootstrapServers: "kafka:9092",
  consumerGroup: "iot-device-dim",
});

const windowed = SlideWindow({ size: "5 MINUTE", slide: "30 SECOND", on: "readingTime", children: readings });

const stats = Aggregate({
  groupBy: ['deviceId', 'sensorType'],
  select: {
    deviceId: 'deviceId',
    sensorType: 'sensorType',
    avgValue: 'AVG(value)',
    stddevValue: 'STDDEV_POP(value)',
    maxValue: 'MAX(value)',
    readingCount: 'COUNT(*)',
    windowEnd: 'window_end',
  },
  children: windowed,
});

const enriched = TemporalJoin({
  stream: stats,
  temporal: devices,
  on: "deviceId = deviceId",
  asOf: "windowEnd",
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
      <Route.Branch condition="1 = 1">
        <JdbcSink table="sensor_dashboard" url="jdbc:postgresql://postgres:5432/flink_sink" />
      </Route.Branch>
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
  ]
}
