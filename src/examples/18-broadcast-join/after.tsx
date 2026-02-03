import { createElement } from '../../core/jsx-runtime';
import { Schema, Field } from '../../core/schema';
import { Pipeline } from '../../components/pipeline';
import { KafkaSource, JdbcSource } from '../../components/sources';
import { KafkaSink } from '../../components/sinks';
import { Join } from '../../components/joins';

const EventSchema = Schema({
  fields: {
    event_id: Field.STRING(),
    user_id: Field.STRING(),
    event_type: Field.STRING(),
    product_id: Field.STRING(),
    event_time: Field.TIMESTAMP(3),
  },
});

const BlacklistSchema = Schema({
  fields: {
    user_id: Field.STRING(),
    reason: Field.STRING(),
    blocked_until: Field.TIMESTAMP(3),
  },
});

const events = (
  <KafkaSource
    topic="user_events"
    bootstrapServers="kafka:9092"
    schema={EventSchema}
  />
);

const blacklist = (
  <JdbcSource
    url="jdbc:postgresql://db:5432/moderation"
    table="blacklist"
    schema={BlacklistSchema}
  />
);

export default (
  <Pipeline name="blacklist-filter" parallelism={32}>
    <Join
      left={events}
      right={blacklist}
      on="user_id = user_id"
      type="anti"
      hints={{ broadcast: 'right' }}
    />
    <KafkaSink topic="valid_events" />
  </Pipeline>
);
