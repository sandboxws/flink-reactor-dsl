import { Pipeline } from "@/components/pipeline"
import { JdbcSink } from "@/components/sinks"
import { KafkaSource } from "@/components/sources"
import { Aggregate, Map } from "@/components/transforms"
import { SlideWindow } from "@/components/windows"
import { createElement } from "@/core/jsx-runtime"
import { Field, Schema } from "@/core/schema"

const SensorSchema = Schema({
  fields: {
    sensor_id: Field.STRING(),
    temperature: Field.DOUBLE(),
    humidity: Field.DOUBLE(),
    reading_time: Field.TIMESTAMP(3),
  },
  watermark: {
    column: "reading_time",
    expression: "reading_time - INTERVAL '30' SECOND",
  },
})

export default (
  <Pipeline name="sensor-metrics" parallelism={16}>
    <KafkaSource
      topic="sensor_readings"
      bootstrapServers="kafka:9092"
      schema={SensorSchema}
    />
    <SlideWindow size="5 minutes" slide="1 minute" on="reading_time">
      <Aggregate
        groupBy={["sensor_id"]}
        select={{
          sensor_id: "sensor_id",
          avg_temp: "AVG(temperature)",
          max_temp: "MAX(temperature)",
          min_temp: "MIN(temperature)",
          avg_humidity: "AVG(humidity)",
        }}
      />
    </SlideWindow>
    <Map
      select={{
        sensor_id: "sensor_id",
        avg_temp: "avg_temp",
        max_temp: "max_temp",
        min_temp: "min_temp",
        avg_humidity: "avg_humidity",
        window_start: "window_start",
        window_end: "window_end",
        temp_variance: "max_temp - min_temp",
      }}
    />
    <JdbcSink url="jdbc:postgresql://db:5433/iot" table="sensor_metrics" />
  </Pipeline>
)
