import { IntervalJoin } from "@/components/joins"
import { Pipeline } from "@/components/pipeline"
import { JdbcSink } from "@/components/sinks"
import { KafkaSource } from "@/components/sources"
import { Aggregate } from "@/components/transforms"
import { createElement } from "@/core/jsx-runtime"
import { Field, Schema } from "@/core/schema"

const PageViewSchema = Schema({
  fields: {
    session_id: Field.STRING(),
    user_id: Field.STRING(),
    page_url: Field.STRING(),
    view_time: Field.TIMESTAMP(3),
  },
  watermark: {
    column: "view_time",
    expression: "view_time - INTERVAL '30' SECOND",
  },
})

const ClickSchema = Schema({
  fields: {
    click_id: Field.STRING(),
    session_id: Field.STRING(),
    element_id: Field.STRING(),
    click_time: Field.TIMESTAMP(3),
  },
  watermark: {
    column: "click_time",
    expression: "click_time - INTERVAL '30' SECOND",
  },
})

const ConversionSchema = Schema({
  fields: {
    conversion_id: Field.STRING(),
    session_id: Field.STRING(),
    product_id: Field.STRING(),
    revenue: Field.DECIMAL(10, 2),
    conversion_time: Field.TIMESTAMP(3),
  },
  watermark: {
    column: "conversion_time",
    expression: "conversion_time - INTERVAL '30' SECOND",
  },
})

const pageViews = (
  <KafkaSource
    topic="page_views"
    bootstrapServers="kafka:9092"
    schema={PageViewSchema}
  />
)

const clicks = (
  <KafkaSource
    topic="clicks"
    bootstrapServers="kafka:9092"
    schema={ClickSchema}
  />
)

const conversions = (
  <KafkaSource
    topic="conversions"
    bootstrapServers="kafka:9092"
    schema={ConversionSchema}
  />
)

// First join: page views + clicks within 30 minutes
const viewsWithClicks = (
  <IntervalJoin
    left={pageViews}
    right={clicks}
    on="session_id = session_id"
    type="left"
    interval={{
      from: "view_time",
      to: "view_time + INTERVAL '30' MINUTE",
    }}
  />
)

// Second join: enriched views + conversions within 1 hour
const fullFunnel = (
  <IntervalJoin
    left={viewsWithClicks}
    right={conversions}
    on="session_id = session_id"
    type="left"
    interval={{
      from: "view_time",
      to: "view_time + INTERVAL '1' HOUR",
    }}
  />
)

export default (
  <Pipeline name="user-funnel" parallelism={8}>
    {fullFunnel}
    <Aggregate
      groupBy={["user_id"]}
      select={{
        user_id: "user_id",
        total_views: "COUNT(*)",
        total_clicks: "COUNT(click_id)",
        total_conversions: "COUNT(conversion_id)",
        total_revenue: "SUM(revenue)",
      }}
    />
    <JdbcSink
      url="jdbc:postgresql://db:5432/analytics"
      table="user_funnel_metrics"
    />
  </Pipeline>
)
