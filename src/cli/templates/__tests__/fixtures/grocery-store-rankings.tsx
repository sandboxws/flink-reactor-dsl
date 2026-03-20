import { Pipeline } from "@/components/pipeline"
import { JdbcSink } from "@/components/sinks"
import { KafkaSource } from "@/components/sources"
import { Aggregate, Deduplicate } from "@/components/transforms"
import { TumbleWindow } from "@/components/windows"
import { createElement } from "@/core/jsx-runtime"
import { Field, Schema } from "@/core/schema"

const RatingSchema = Schema({
  fields: {
    orderId: Field.STRING(),
    storeId: Field.STRING(),
    shopperRating: Field.DOUBLE(),
    storeRating: Field.DOUBLE(),
    itemQuality: Field.DOUBLE(),
    ratingTime: Field.TIMESTAMP(3),
  },
  watermark: {
    column: "ratingTime",
    expression: "ratingTime - INTERVAL '5' SECOND",
  },
})

export default (
  <Pipeline name="grocery-store-rankings" mode="streaming">
    <KafkaSource
      topic="grocery.ratings"
      schema={RatingSchema}
      bootstrapServers="kafka:9092"
      consumerGroup="grocery-rankings"
    />
    <Deduplicate key={["orderId"]} order="ratingTime" keep="first" />
    <TumbleWindow size="15 MINUTE" on="ratingTime" />
    <Aggregate
      groupBy={["storeId"]}
      select={{
        storeId: "storeId",
        avgRating: "AVG(storeRating)",
        ratingCount: "COUNT(*)",
        windowEnd: "window_end",
      }}
    />
    <JdbcSink
      table="store_rankings"
      url="jdbc:postgresql://postgres:5432/flink_sink"
      upsertMode
      keyFields={["storeId"]}
    />
  </Pipeline>
)
