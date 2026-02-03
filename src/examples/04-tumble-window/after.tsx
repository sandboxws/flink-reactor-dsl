import { createElement } from '../../core/jsx-runtime';
import { Schema, Field } from '../../core/schema';
import { Pipeline } from '../../components/pipeline';
import { KafkaSource } from '../../components/sources';
import { KafkaSink } from '../../components/sinks';
import { Aggregate, Filter } from '../../components/transforms';
import { TumbleWindow } from '../../components/windows';

const ClickstreamSchema = Schema({
  fields: {
    user_id: Field.STRING(),
    page_url: Field.STRING(),
    session_id: Field.STRING(),
    event_time: Field.TIMESTAMP(3),
  },
  watermark: {
    column: 'event_time',
    expression: "event_time - INTERVAL '10' SECOND",
  },
});

export default (
  <Pipeline name="active-users-per-minute" parallelism={12}>
    <KafkaSource
      topic="clickstream"
      bootstrapServers="kafka:9092"
      schema={ClickstreamSchema}
    />
    <TumbleWindow size="1 minute" on="event_time">
      <Aggregate
        groupBy={['user_id']}
        select={{
          user_id: 'user_id',
          page_views: 'COUNT(*)',
          unique_pages: 'COUNT(DISTINCT page_url)',
        }}
      />
    </TumbleWindow>
    <Filter condition="page_views > 5" />
    <KafkaSink topic="active_users_per_minute" />
  </Pipeline>
);
