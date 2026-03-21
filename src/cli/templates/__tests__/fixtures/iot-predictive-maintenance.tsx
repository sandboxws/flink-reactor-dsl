import { TemporalJoin } from "@/components/joins"
import { Pipeline } from "@/components/pipeline"
import { Route } from "@/components/route"
import { JdbcSink, KafkaSink } from "@/components/sinks"
import { KafkaSource } from "@/components/sources"
import { Aggregate } from "@/components/transforms"
import { SlideWindow } from "@/components/windows"
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

const readings = KafkaSource({
  topic: "iot.sensor-readings",
  schema: SensorReadingSchema,
  bootstrapServers: "kafka:9092",
  consumerGroup: "iot-maintenance",
})

const devices = KafkaSource({
  topic: "iot.device-registry",
  schema: DeviceRegistrySchema,
  format: "debezium-json",
  bootstrapServers: "kafka:9092",
  consumerGroup: "iot-device-dim",
})

const windowed = SlideWindow({
  size: "5 MINUTE",
  slide: "30 SECOND",
  on: "readingTime",
  children: readings,
})

const stats = Aggregate({
  groupBy: ["deviceId", "sensorType"],
  select: {
    deviceId: "deviceId",
    sensorType: "sensorType",
    avgValue: "AVG(value)",
    stddevValue: "STDDEV_POP(value)",
    maxValue: "MAX(value)",
    readingCount: "COUNT(*)",
    windowEnd: "window_end",
  },
  children: windowed,
})

const enriched = TemporalJoin({
  stream: stats,
  temporal: devices,
  on: "deviceId = deviceId",
  asOf: "windowEnd",
})

export default (
  <Pipeline name="iot-predictive-maintenance" mode="streaming">
    {readings}
    {devices}
    {enriched}
    <Route>
      <Route.Branch condition="stddevValue > thresholdTemp">
        <KafkaSink
          topic="iot.maintenance-alerts"
          bootstrapServers="kafka:9092"
        />
      </Route.Branch>
      <Route.Branch condition="1 = 1">
        <JdbcSink
          table="sensor_dashboard"
          url="jdbc:postgresql://postgres:5432/flink_sink"
        />
      </Route.Branch>
    </Route>
  </Pipeline>
)
