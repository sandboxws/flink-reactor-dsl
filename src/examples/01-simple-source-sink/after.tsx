import { Pipeline } from "@/components/pipeline"
import { KafkaSink } from "@/components/sinks"
import { KafkaSource } from "@/components/sources"
import { createElement } from "@/core/jsx-runtime"
import { Field, Schema } from "@/core/schema"

const UserEventSchema = Schema({
  fields: {
    event_id: Field.STRING(),
    user_id: Field.STRING(),
    event_type: Field.STRING(),
    payload: Field.STRING(),
    event_time: Field.TIMESTAMP(3),
  },
})

export default (
  <Pipeline name="simple-source-sink" parallelism={4}>
    <KafkaSource
      topic="user_events"
      bootstrapServers="kafka:9092"
      schema={UserEventSchema}
    />
    <KafkaSink topic="user_events_processed" />
  </Pipeline>
)
