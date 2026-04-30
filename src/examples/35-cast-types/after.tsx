import { Cast } from "@/components/field-transforms"
import { Pipeline } from "@/components/pipeline"
import { KafkaSink } from "@/components/sinks"
import { KafkaSource } from "@/components/sources"
import { Field, Schema } from "@/core/schema"

const RawSensorSchema = Schema({
  fields: {
    sensor_id: Field.STRING(),
    temperature: Field.STRING(),
    humidity: Field.STRING(),
    reading_time: Field.STRING(),
  },
})

export default (
  <Pipeline name="cast-sensor-readings" parallelism={4}>
    <KafkaSource
      topic="raw_sensor_data"
      bootstrapServers="kafka:9092"
      schema={RawSensorSchema}
    />
    <Cast
      columns={{
        temperature: "DOUBLE",
        humidity: "DOUBLE",
        reading_time: "TIMESTAMP(3)",
      }}
      safe={true}
    />
    <KafkaSink topic="typed_sensor_data" />
  </Pipeline>
)
