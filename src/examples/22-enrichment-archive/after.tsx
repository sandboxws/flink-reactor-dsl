import { LookupJoin } from "../../components/joins"
import { Pipeline } from "../../components/pipeline"
import { Route } from "../../components/route"
import { FileSystemSink, KafkaSink } from "../../components/sinks"
import { KafkaSource } from "../../components/sources"
import { Filter } from "../../components/transforms"
import { createElement } from "../../core/jsx-runtime"
import { Field, Schema } from "../../core/schema"

const EventSchema = Schema({
  fields: {
    event_id: Field.STRING(),
    user_id: Field.STRING(),
    event_type: Field.STRING(),
    event_data: Field.STRING(),
    event_time: Field.TIMESTAMP(3),
    processed_time: Field.TIMESTAMP(3),
  },
  watermark: {
    column: "event_time",
    expression: "event_time - INTERVAL '30' SECOND",
  },
})

const events = (
  <KafkaSource
    topic="user_events"
    bootstrapServers="kafka:9092"
    schema={EventSchema}
  />
)

// Enrich with user profiles
const enriched = (
  <LookupJoin
    input={events}
    table="user_profiles"
    url="jdbc:postgresql://db:5432/users"
    on="user_id"
    async={{ enabled: true, capacity: 100 }}
    cache={{ type: "lru", maxRows: 50000, ttl: "5m" }}
  />
)

export default (
  <Pipeline name="enrichment-archive" parallelism={12}>
    {events}
    <Route>
      {/* Branch 1: Raw archive to S3 */}
      <Route.Branch condition="true">
        <FileSystemSink
          path="s3://data-lake/raw/user_events"
          format="parquet"
          partitionBy={["DATE(event_time)"]}
          rollingPolicy={{ size: "128MB", interval: "15min" }}
        />
      </Route.Branch>

      {/* Branch 2: Enriched events */}
      <Route.Branch condition="true">
        {enriched}
        <KafkaSink topic="enriched_user_events" />
      </Route.Branch>

      {/* Branch 3: Premium conversions */}
      <Route.Branch condition="true">
        {enriched}
        <Filter condition="user_tier = 'premium' AND event_type IN ('purchase', 'subscription')" />
        <KafkaSink topic="premium_user_conversions" />
      </Route.Branch>
    </Route>
  </Pipeline>
)
