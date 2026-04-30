import { Rename } from "@/components/field-transforms"
import { Pipeline } from "@/components/pipeline"
import { KafkaSink } from "@/components/sinks"
import { KafkaSource } from "@/components/sources"
import { Field, Schema } from "@/core/schema"

const LegacySchema = Schema({
  fields: {
    usr_id: Field.STRING(),
    evt_type: Field.STRING(),
    ts: Field.TIMESTAMP(3),
    payload: Field.STRING(),
  },
})

export default (
  <Pipeline name="standardize-columns" parallelism={4}>
    <KafkaSource
      topic="legacy_events"
      bootstrapServers="kafka:9092"
      schema={LegacySchema}
    />
    <Rename
      columns={{
        usr_id: "user_id",
        evt_type: "event_type",
        ts: "event_time",
      }}
    />
    <KafkaSink topic="standardized_events" />
  </Pipeline>
)
