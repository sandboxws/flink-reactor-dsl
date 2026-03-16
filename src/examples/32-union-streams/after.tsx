import { Pipeline } from "@/components/pipeline"
import { KafkaSink } from "@/components/sinks"
import { KafkaSource } from "@/components/sources"
import { Union } from "@/components/transforms"
import { createElement } from "@/core/jsx-runtime"
import { Field, Schema } from "@/core/schema"

const RegionalEventSchema = Schema({
  fields: {
    event_id: Field.STRING(),
    user_id: Field.STRING(),
    event_type: Field.STRING(),
    region: Field.STRING(),
    event_time: Field.TIMESTAMP(3),
  },
})

const us = (
  <KafkaSource
    topic="events_us"
    bootstrapServers="kafka-us:9092"
    schema={RegionalEventSchema}
  />
)

const eu = (
  <KafkaSource
    topic="events_eu"
    bootstrapServers="kafka-eu:9092"
    schema={RegionalEventSchema}
  />
)

export default (
  <Pipeline name="merged-regional-events" parallelism={8}>
    <Union>
      {us}
      {eu}
    </Union>
    <KafkaSink topic="events_global" />
  </Pipeline>
)
