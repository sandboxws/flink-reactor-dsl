import { Pipeline } from "@/components/pipeline"
import { Route } from "@/components/route"
import { FileSystemSink, JdbcSink, KafkaSink } from "@/components/sinks"
import { KafkaSource } from "@/components/sources"
import { Aggregate } from "@/components/transforms"
import { TumbleWindow } from "@/components/windows"
import { Field, Schema } from "@/core/schema"

const SensorSchema = Schema({
  fields: {
    device_id: Field.STRING(),
    sensor_type: Field.STRING(),
    reading_value: Field.DOUBLE(),
    reading_time: Field.TIMESTAMP(3),
    location_id: Field.STRING(),
  },
  watermark: {
    column: "reading_time",
    expression: "reading_time - INTERVAL '30' SECOND",
  },
})

export default (
  <Pipeline name="iot-sensor-pipeline">
    <KafkaSource
      topic="iot_sensor_data"
      bootstrapServers="kafka:9092"
      schema={SensorSchema}
    />
    <Route>
      {/* High temperature alerts */}
      <Route.Branch condition="sensor_type = 'temperature' AND reading_value > 100">
        <KafkaSink topic="high_temp_alerts" />
      </Route.Branch>

      {/* 5-minute windowed metrics */}
      <Route.Branch condition="true">
        <TumbleWindow size="5 minutes" on="reading_time">
          <Aggregate
            groupBy={["device_id", "sensor_type"]}
            select={{
              device_id: "device_id",
              sensor_type: "sensor_type",
              avg_value: "AVG(reading_value)",
              min_value: "MIN(reading_value)",
              max_value: "MAX(reading_value)",
            }}
          />
        </TumbleWindow>
        <JdbcSink
          url="jdbc:postgresql://db:5432/iot"
          table="sensor_metrics_5min"
        />
      </Route.Branch>

      {/* Raw archive to S3 */}
      <Route.Default>
        <FileSystemSink
          path="/tmp/iot-archive/raw/"
          format="json"
          partitionBy={["DATE(reading_time)"]}
        />
      </Route.Default>
    </Route>
  </Pipeline>
)
