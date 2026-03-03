import { LookupJoin } from "@/components/joins"
import { Pipeline } from "@/components/pipeline"
import { KafkaSink } from "@/components/sinks"
import { KafkaSource } from "@/components/sources"
import { Filter } from "@/components/transforms"
import { createElement } from "@/core/jsx-runtime"
import { Field, Schema } from "@/core/schema"

const ActionSchema = Schema({
  fields: {
    action_id: Field.STRING(),
    user_id: Field.STRING(),
    action_type: Field.STRING(),
    action_time: Field.TIMESTAMP(3),
    metadata: Field.STRING(),
  },
})

const actions = (
  <KafkaSource
    topic="user_actions"
    bootstrapServers="kafka:9092"
    schema={ActionSchema}
  />
)

export default (
  <Pipeline name="premium-user-enrichment" parallelism={16}>
    <LookupJoin
      input={actions}
      table="user_profiles"
      url="jdbc:mysql://db:3306/users"
      on="user_id"
      async={{ enabled: true, capacity: 100, timeout: "30s" }}
      cache={{ type: "lru", maxRows: 10000, ttl: "1m" }}
    />
    <Filter condition="user_tier IN ('premium', 'enterprise')" />
    <KafkaSink topic="premium_user_actions" />
  </Pipeline>
)
