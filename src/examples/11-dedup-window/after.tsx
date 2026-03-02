import { Pipeline } from "../../components/pipeline"
import { KafkaSink } from "../../components/sinks"
import { KafkaSource } from "../../components/sources"
import { Aggregate, Deduplicate } from "../../components/transforms"
import { TumbleWindow } from "../../components/windows"
import { createElement } from "../../core/jsx-runtime"
import { Field, Schema } from "../../core/schema"

const RawEventSchema = Schema({
  fields: {
    event_id: Field.STRING(),
    user_id: Field.STRING(),
    event_type: Field.STRING(),
    event_data: Field.STRING(),
    event_time: Field.TIMESTAMP(3),
  },
  watermark: {
    column: "event_time",
    expression: "event_time - INTERVAL '5' SECOND",
  },
})

export default (
  <Pipeline name="hourly-user-events" parallelism={16}>
    <KafkaSource
      topic="raw_events"
      bootstrapServers="kafka:9092"
      schema={RawEventSchema}
    />
    <Deduplicate key={["event_id"]} order="event_time" keep="first" />
    <TumbleWindow size="1 hour" on="event_time">
      <Aggregate
        groupBy={["user_id", "event_type"]}
        select={{
          user_id: "user_id",
          event_type: "event_type",
          event_count: "COUNT(*)",
        }}
      />
    </TumbleWindow>
    <KafkaSink topic="hourly_user_events" />
  </Pipeline>
)
