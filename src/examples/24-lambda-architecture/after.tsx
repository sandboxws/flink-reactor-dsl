import { Pipeline } from "../../components/pipeline"
import { Route } from "../../components/route"
import { FileSystemSink, JdbcSink, KafkaSink } from "../../components/sinks"
import { KafkaSource } from "../../components/sources"
import { Aggregate } from "../../components/transforms"
import { TumbleWindow } from "../../components/windows"
import { createElement } from "../../core/jsx-runtime"
import { Field, Schema } from "../../core/schema"

const ClickstreamSchema = Schema({
  fields: {
    session_id: Field.STRING(),
    user_id: Field.STRING(),
    page_url: Field.STRING(),
    referrer: Field.STRING(),
    event_type: Field.STRING(),
    event_time: Field.TIMESTAMP(3),
    user_agent: Field.STRING(),
    ip_address: Field.STRING(),
  },
  watermark: {
    column: "event_time",
    expression: "event_time - INTERVAL '10' SECOND",
  },
})

export default (
  <Pipeline name="clickstream-lambda" parallelism={24}>
    <KafkaSource
      topic="clickstream"
      bootstrapServers="kafka:9092"
      schema={ClickstreamSchema}
    />
    <Route>
      {/* Sink 1: Raw archive to data lake */}
      <Route.Branch condition="true">
        <FileSystemSink
          path="s3://data-lake/clickstream/raw/"
          format="parquet"
          partitionBy={["DATE(event_time)", "HOUR(event_time)"]}
          rollingPolicy={{ size: "256MB", interval: "10min" }}
        />
      </Route.Branch>

      {/* Sink 2: Real-time page metrics */}
      <Route.Branch condition="true">
        <TumbleWindow size="1 minute" on="event_time">
          <Aggregate
            groupBy={["page_url"]}
            select={{
              page_url: "page_url",
              view_count: "COUNT(*)",
              unique_visitors: "COUNT(DISTINCT user_id)",
            }}
          />
        </TumbleWindow>
        <KafkaSink topic="realtime_page_metrics" />
      </Route.Branch>

      {/* Sink 3: User activity upsert */}
      <Route.Branch condition="true">
        <Aggregate
          groupBy={["user_id"]}
          select={{
            user_id: "user_id",
            total_events: "COUNT(*)",
            session_count: "COUNT(DISTINCT session_id)",
            last_seen: "MAX(event_time)",
          }}
        />
        <JdbcSink
          url="jdbc:postgresql://db:5432/analytics"
          table="user_activity_summary"
          upsertMode={true}
          keyFields={["user_id"]}
        />
      </Route.Branch>

      {/* Sink 4: Error alerts */}
      <Route.Branch condition="event_type IN ('error', 'exception')">
        <KafkaSink topic="error_events_alerts" />
      </Route.Branch>
    </Route>
  </Pipeline>
)
