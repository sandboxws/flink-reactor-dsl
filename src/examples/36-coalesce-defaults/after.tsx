import { Coalesce } from "@/components/field-transforms"
import { Pipeline } from "@/components/pipeline"
import { KafkaSink } from "@/components/sinks"
import { KafkaSource } from "@/components/sources"
import { Field, Schema } from "@/core/schema"

const UserProfileSchema = Schema({
  fields: {
    user_id: Field.STRING(),
    display_name: Field.STRING(),
    locale: Field.STRING(),
    timezone: Field.STRING(),
    updated_at: Field.TIMESTAMP(3),
  },
})

export default (
  <Pipeline name="fill-profile-defaults" parallelism={4}>
    <KafkaSource
      topic="user_profiles"
      bootstrapServers="kafka:9092"
      schema={UserProfileSchema}
    />
    <Coalesce
      columns={{
        display_name: "user_id",
        locale: "'en-US'",
        timezone: "'UTC'",
      }}
    />
    <KafkaSink topic="enriched_profiles" />
  </Pipeline>
)
