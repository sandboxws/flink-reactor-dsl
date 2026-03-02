import { Pipeline } from "../../components/pipeline"
import { JdbcSink } from "../../components/sinks"
import { KafkaSource } from "../../components/sources"
import { Aggregate, Map, Union } from "../../components/transforms"
import { TumbleWindow } from "../../components/windows"
import { createElement } from "../../core/jsx-runtime"
import { Field, Schema } from "../../core/schema"

const MobileEventSchema = Schema({
  fields: {
    event_id: Field.STRING(),
    user_id: Field.STRING(),
    event_type: Field.STRING(),
    event_time: Field.TIMESTAMP(3),
  },
  watermark: {
    column: "event_time",
    expression: "event_time - INTERVAL '30' SECOND",
  },
})

const WebEventSchema = Schema({
  fields: {
    event_id: Field.STRING(),
    user_id: Field.STRING(),
    event_type: Field.STRING(),
    event_time: Field.TIMESTAMP(3),
  },
  watermark: {
    column: "event_time",
    expression: "event_time - INTERVAL '30' SECOND",
  },
})

const IoTEventSchema = Schema({
  fields: {
    event_id: Field.STRING(),
    device_id: Field.STRING(),
    event_type: Field.STRING(),
    event_time: Field.TIMESTAMP(3),
  },
  watermark: {
    column: "event_time",
    expression: "event_time - INTERVAL '30' SECOND",
  },
})

const mobile = (
  <KafkaSource
    topic="mobile_events"
    bootstrapServers="kafka:9092"
    schema={MobileEventSchema}
  >
    <Map
      select={{
        event_id: "event_id",
        user_id: "user_id",
        event_type: "event_type",
        event_time: "event_time",
        platform: "'mobile'",
      }}
    />
  </KafkaSource>
)

const web = (
  <KafkaSource
    topic="web_events"
    bootstrapServers="kafka:9092"
    schema={WebEventSchema}
  >
    <Map
      select={{
        event_id: "event_id",
        user_id: "user_id",
        event_type: "event_type",
        event_time: "event_time",
        platform: "'web'",
      }}
    />
  </KafkaSource>
)

const iot = (
  <KafkaSource
    topic="iot_events"
    bootstrapServers="kafka:9092"
    schema={IoTEventSchema}
  >
    <Map
      select={{
        event_id: "event_id",
        user_id: "device_id",
        event_type: "event_type",
        event_time: "event_time",
        platform: "'iot'",
      }}
    />
  </KafkaSource>
)

export default (
  <Pipeline name="platform-hourly-metrics" parallelism={8}>
    <Union>
      {mobile}
      {web}
      {iot}
    </Union>
    <TumbleWindow size="1 hour" on="event_time">
      <Aggregate
        groupBy={["platform", "event_type"]}
        select={{
          platform: "platform",
          event_type: "event_type",
          event_count: "COUNT(*)",
          unique_users: "COUNT(DISTINCT user_id)",
        }}
      />
    </TumbleWindow>
    <JdbcSink
      url="jdbc:postgresql://db:5432/analytics"
      table="platform_hourly_metrics"
    />
  </Pipeline>
)
