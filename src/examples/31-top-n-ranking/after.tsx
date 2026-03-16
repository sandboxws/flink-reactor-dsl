import { Pipeline } from "@/components/pipeline"
import { KafkaSink } from "@/components/sinks"
import { KafkaSource } from "@/components/sources"
import { Aggregate, TopN } from "@/components/transforms"
import { createElement } from "@/core/jsx-runtime"
import { Field, Schema } from "@/core/schema"

const SalesSchema = Schema({
  fields: {
    product_id: Field.STRING(),
    category: Field.STRING(),
    revenue: Field.DECIMAL(10, 2),
    sale_time: Field.TIMESTAMP(3),
  },
  watermark: {
    column: "sale_time",
    expression: "sale_time - INTERVAL '5' SECOND",
  },
})

export default (
  <Pipeline name="top-products-by-category" parallelism={8}>
    <KafkaSource
      topic="sales"
      bootstrapServers="kafka:9092"
      schema={SalesSchema}
    />
    <Aggregate
      groupBy={["category", "product_id"]}
      select={{
        category: "category",
        product_id: "product_id",
        total_revenue: "SUM(revenue)",
      }}
    />
    <TopN
      partitionBy={["category"]}
      orderBy={{ total_revenue: "DESC" }}
      n={3}
    />
    <KafkaSink topic="top_products" />
  </Pipeline>
)
