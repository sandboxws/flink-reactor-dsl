import { Pipeline } from "@/components/pipeline"
import { JdbcSink } from "@/components/sinks"
import { KafkaSource } from "@/components/sources"
import { Aggregate } from "@/components/transforms"
import { TumbleWindow } from "@/components/windows"
import { createElement } from "@/core/jsx-runtime"
import { Field, Schema } from "@/core/schema"

const PageViewSchema = Schema({
  fields: {
    userId: Field.STRING(),
    pageUrl: Field.STRING(),
    viewTimestamp: Field.TIMESTAMP(3),
  },
  watermark: {
    column: "viewTimestamp",
    expression: "viewTimestamp - INTERVAL '5' SECOND",
  },
})

export default (
  <Pipeline name="page-view-analytics">
    <KafkaSource
      topic="page-views"
      schema={PageViewSchema}
      bootstrapServers="kafka:9092"
      consumerGroup="analytics"
    />
    <TumbleWindow size="1 MINUTE" on="viewTimestamp" />
    <Aggregate
      groupBy={["pageUrl"]}
      select={{
        pageUrl: "pageUrl",
        viewCount: "COUNT(*)",
        windowStart: "window_start",
        windowEnd: "window_end",
      }}
    />
    <JdbcSink
      table="page_view_stats"
      url="jdbc:postgresql://localhost:5432/analytics"
    />
  </Pipeline>
)
