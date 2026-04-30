import { LookupJoin } from "@/components/joins"
import { Pipeline } from "@/components/pipeline"
import { KafkaSink } from "@/components/sinks"
import { KafkaSource } from "@/components/sources"
import { Aggregate, TopN } from "@/components/transforms"
import { TumbleWindow } from "@/components/windows"
import { Field, Schema } from "@/core/schema"

const SalesSchema = Schema({
  fields: {
    transaction_id: Field.STRING(),
    store_id: Field.STRING(),
    product_id: Field.STRING(),
    quantity: Field.INT(),
    unit_price: Field.DECIMAL(10, 2),
    transaction_time: Field.TIMESTAMP(3),
  },
  watermark: {
    column: "transaction_time",
    expression: "transaction_time - INTERVAL '5' SECOND",
  },
})

const sales = (
  <KafkaSource
    topic="sales_transactions"
    bootstrapServers="kafka:9092"
    schema={SalesSchema}
  />
)

// Enrich with product dimension
const withProducts = (
  <LookupJoin
    input={sales}
    table="dim_products"
    url="jdbc:mysql://db:3306/catalog"
    on="product_id"
    async={{ enabled: true, capacity: 200 }}
    cache={{ type: "lru", maxRows: 50000, ttl: "5m" }}
  />
)

// Enrich with store dimension
const enriched = (
  <LookupJoin
    input={withProducts}
    table="dim_stores"
    url="jdbc:mysql://db:3306/catalog"
    on="store_id"
    async={{ enabled: true, capacity: 100 }}
    cache={{ type: "lru", maxRows: 10000, ttl: "10m" }}
  />
)

export default (
  <Pipeline name="dashboard-top-categories" parallelism={16}>
    {enriched}
    <TumbleWindow size="1 minute" on="transaction_time">
      <Aggregate
        groupBy={["region", "category"]}
        select={{
          region: "region",
          category: "category",
          revenue: "SUM(quantity * unit_price)",
          units_sold: "SUM(quantity)",
          transaction_count: "COUNT(*)",
        }}
      />
    </TumbleWindow>
    <TopN
      partitionBy={["window_start", "region"]}
      orderBy={{ revenue: "DESC" }}
      n={5}
    />
    <KafkaSink topic="dashboard_top_categories_per_region" />
  </Pipeline>
)
