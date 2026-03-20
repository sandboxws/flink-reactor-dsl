import type { ScaffoldOptions, TemplateFile } from "@/cli/commands/new.js"
import { sharedFiles } from "./shared.js"

export function getIotFactoryTemplates(opts: ScaffoldOptions): TemplateFile[] {
  return [
    ...sharedFiles(opts),
    {
      path: "schemas/iot.ts",
      content: `import { Schema, Field } from 'flink-reactor';

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
} from 'flink-reactor';
import { SensorReadingSchema, DeviceRegistrySchema } from '@/schemas/iot';

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
    <KafkaSource topic="iot.sensor-readings" schema={SensorReadingSchema} bootstrapServers="kafka:9092" consumerGroup="iot-maintenance" />
    <SlideWindow size="5 MINUTE" slide="30 SECOND" on="readingTime" />
    <Aggregate
      groupBy={['deviceId', 'sensorType']}
      select={{
        deviceId: 'deviceId',
        sensorType: 'sensorType',
        avgValue: 'AVG(value)',
        stddevValue: 'STDDEV_POP(value)',
        maxValue: 'MAX(value)',
        readingCount: 'COUNT(*)',
        windowEnd: 'WINDOW_END',
      }}
    />
    <TemporalJoin
      rightSource={<KafkaSource topic="iot.device-registry" schema={DeviceRegistrySchema} format="debezium-json" bootstrapServers="kafka:9092" consumerGroup="iot-device-dim" />}
      on="deviceId"
    />
    <Route>
      <KafkaSink topic="iot.maintenance-alerts" bootstrapServers="kafka:9092" condition="stddevValue > thresholdTemp" />
      <JdbcSink table="sensor_dashboard" url="jdbc:postgresql://postgres:5432/flink_sink" />
    </Route>
  </Pipeline>
);
`,
    },
    {
      path: "pipelines/pump-iot/index.tsx",
      content: `import {
  Pipeline, DataGenSource, KafkaSink, StatementSet,
} from 'flink-reactor';
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
