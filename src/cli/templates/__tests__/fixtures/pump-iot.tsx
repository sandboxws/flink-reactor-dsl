import { Pipeline } from "@/components/pipeline"
import { KafkaSink } from "@/components/sinks"
import { DataGenSource } from "@/components/sources"
import { StatementSet } from "@/components/statement-set"
import { createElement } from "@/core/jsx-runtime"
import { Field, Schema } from "@/core/schema"

const SensorReadingSchema = Schema({
  fields: {
    deviceId: Field.STRING(),
    sensorType: Field.STRING(),
    value: Field.DOUBLE(),
    unit: Field.STRING(),
    readingTime: Field.TIMESTAMP(3),
  },
  watermark: {
    column: "readingTime",
    expression: "readingTime - INTERVAL '5' SECOND",
  },
})

const DeviceRegistrySchema = Schema({
  fields: {
    deviceId: Field.STRING(),
    location: Field.STRING(),
    firmwareVersion: Field.STRING(),
    installDate: Field.STRING(),
    thresholdTemp: Field.DOUBLE(),
    thresholdVibration: Field.DOUBLE(),
    updateTime: Field.TIMESTAMP(3),
  },
  primaryKey: { columns: ["deviceId"] },
})

export default (
  <Pipeline name="pump-iot" mode="streaming">
    <StatementSet>
      <DataGenSource schema={SensorReadingSchema} rowsPerSecond={5000} />
      <KafkaSink topic="iot.sensor-readings" bootstrapServers="kafka:9092" />

      <DataGenSource schema={DeviceRegistrySchema} rowsPerSecond={20} />
      <KafkaSink topic="iot.device-registry" bootstrapServers="kafka:9092" />
    </StatementSet>
  </Pipeline>
)
