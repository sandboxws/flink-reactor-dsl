import { Pipeline } from "@/components/pipeline"
import { Route } from "@/components/route"
import { JdbcSink, KafkaSink } from "@/components/sinks"
import { KafkaSource } from "@/components/sources"
import { Aggregate } from "@/components/transforms"
import { createElement } from "@/core/jsx-runtime"
import { Field, Schema } from "@/core/schema"

const ProductCdcSchema = Schema({
  fields: {
    product_id: Field.BIGINT(),
    product_name: Field.STRING(),
    category: Field.STRING(),
    price: Field.DECIMAL(10, 2),
    stock_quantity: Field.INT(),
    updated_at: Field.TIMESTAMP(3),
  },
  primaryKey: { columns: ["product_id"] },
})

export default (
  <Pipeline name="cdc-product-sync" parallelism={8}>
    <KafkaSource
      topic="mysql.inventory.products"
      bootstrapServers="kafka:9092"
      format="debezium-json"
      schema={ProductCdcSchema}
      startupMode="earliest-offset"
    />
    <Route>
      {/* Sink 1: PostgreSQL replica */}
      <Route.Branch condition="true">
        <JdbcSink
          url="jdbc:postgresql://db:5433/replica"
          table="products_replica"
          upsertMode={true}
          keyFields={["product_id"]}
        />
      </Route.Branch>

      {/* Sink 2: Changelog archive */}
      <Route.Branch condition="true">
        <KafkaSink topic="products_changelog_archive" />
      </Route.Branch>

      {/* Sink 3: Low stock alerts */}
      <Route.Branch condition="stock_quantity < 10">
        <KafkaSink topic="low_stock_alerts" />
      </Route.Branch>

      {/* Sink 4: Category stats */}
      <Route.Branch condition="true">
        <Aggregate
          groupBy={["category"]}
          select={{
            category: "category",
            product_count: "COUNT(*)",
            avg_price: "AVG(price)",
            last_update: "MAX(updated_at)",
          }}
        />
        <JdbcSink
          url="jdbc:postgresql://db:5433/analytics"
          table="category_stats"
          upsertMode={true}
          keyFields={["category"]}
        />
      </Route.Branch>
    </Route>
  </Pipeline>
)
