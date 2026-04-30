import { Drop } from "@/components/field-transforms"
import { Pipeline } from "@/components/pipeline"
import { KafkaSink } from "@/components/sinks"
import { KafkaSource } from "@/components/sources"
import { Field, Schema } from "@/core/schema"

const InternalEventSchema = Schema({
  fields: {
    event_id: Field.STRING(),
    user_id: Field.STRING(),
    event_type: Field.STRING(),
    internal_trace_id: Field.STRING(),
    debug_flags: Field.STRING(),
    raw_payload: Field.STRING(),
    event_time: Field.TIMESTAMP(3),
  },
})

export default (
  <Pipeline name="strip-internal-fields" parallelism={4}>
    <KafkaSource
      topic="internal_events"
      bootstrapServers="kafka:9092"
      schema={InternalEventSchema}
    />
    <Drop columns={["internal_trace_id", "debug_flags", "raw_payload"]} />
    <KafkaSink topic="public_events" />
  </Pipeline>
)
