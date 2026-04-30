import { Pipeline } from "@/components/pipeline"
import { Route } from "@/components/route"
import { FileSystemSink } from "@/components/sinks"
import { GenericSource } from "@/components/sources"
import { Aggregate, Map } from "@/components/transforms"
import { Field, Schema } from "@/core/schema"

const InteractionSchema = Schema({
  fields: {
    user_id: Field.STRING(),
    item_id: Field.STRING(),
    interaction_type: Field.STRING(),
    interaction_time: Field.TIMESTAMP(3),
    duration_seconds: Field.DOUBLE(),
  },
})

export default (
  <Pipeline name="ml-feature-pipeline" mode="batch" parallelism={32}>
    <GenericSource
      connector="filesystem"
      format="parquet"
      schema={InteractionSchema}
      options={{ path: "s3://ml-data/user_interactions/" }}
    />
    <Route>
      {/* Feature table 1: User engagement */}
      <Route.Branch condition="true">
        <Aggregate
          groupBy={["user_id"]}
          select={{
            user_id: "user_id",
            total_interactions: "COUNT(*)",
            unique_items: "COUNT(DISTINCT item_id)",
            total_duration: "SUM(duration_seconds)",
            avg_duration: "AVG(duration_seconds)",
          }}
        />
        <FileSystemSink
          path="s3://ml-features/user_engagement_features/"
          format="parquet"
        />
      </Route.Branch>

      {/* Feature table 2: Item popularity */}
      <Route.Branch condition="true">
        <Aggregate
          groupBy={["item_id"]}
          select={{
            item_id: "item_id",
            view_count: "COUNT(*)",
            unique_viewers: "COUNT(DISTINCT user_id)",
            purchase_count:
              "SUM(CASE WHEN interaction_type = 'purchase' THEN 1 ELSE 0 END)",
          }}
        />
        <Map
          select={{
            item_id: "item_id",
            view_count: "view_count",
            unique_viewers: "unique_viewers",
            purchase_count: "purchase_count",
            conversion_rate:
              "CAST(purchase_count AS DOUBLE) / CAST(view_count AS DOUBLE)",
          }}
        />
        <FileSystemSink
          path="s3://ml-features/item_popularity_features/"
          format="parquet"
        />
      </Route.Branch>

      {/* Feature table 3: User-item pairs */}
      <Route.Branch condition="true">
        <Aggregate
          groupBy={["user_id", "item_id"]}
          select={{
            user_id: "user_id",
            item_id: "item_id",
            interaction_count: "COUNT(*)",
            last_interaction: "MAX(interaction_time)",
          }}
        />
        <FileSystemSink
          path="s3://ml-features/user_item_pairs/"
          format="parquet"
        />
      </Route.Branch>
    </Route>
  </Pipeline>
)
