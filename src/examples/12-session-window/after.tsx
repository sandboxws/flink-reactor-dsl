import { createElement } from '../../core/jsx-runtime';
import { Schema, Field } from '../../core/schema';
import { Pipeline } from '../../components/pipeline';
import { KafkaSource } from '../../components/sources';
import { JdbcSink } from '../../components/sinks';
import { Aggregate, Map } from '../../components/transforms';
import { SessionWindow } from '../../components/windows';

const UserActivitySchema = Schema({
  fields: {
    user_id: Field.STRING(),
    activity_type: Field.STRING(),
    page_url: Field.STRING(),
    activity_time: Field.TIMESTAMP(3),
    device_info: Field.STRING(),
  },
  watermark: {
    column: 'activity_time',
    expression: "activity_time - INTERVAL '1' MINUTE",
  },
});

export default (
  <Pipeline name="user-sessions" parallelism={12}>
    <KafkaSource
      topic="user_activity"
      bootstrapServers="kafka:9092"
      schema={UserActivitySchema}
    />
    <SessionWindow gap="30 minutes" on="activity_time">
      <Aggregate
        groupBy={['user_id']}
        select={{
          user_id: 'user_id',
          total_activities: 'COUNT(*)',
          unique_pages: 'COUNT(DISTINCT page_url)',
          session_start: 'MIN(activity_time)',
          session_end: 'MAX(activity_time)',
          activity_sequence: 'LISTAGG(activity_type)',
        }}
      />
    </SessionWindow>
    <Map select={{
      user_id: 'user_id',
      total_activities: 'total_activities',
      unique_pages: 'unique_pages',
      session_start: 'session_start',
      session_end: 'session_end',
      session_duration: 'session_end - session_start',
      activity_sequence: 'activity_sequence',
    }} />
    <JdbcSink
      url="jdbc:postgresql://db:5432/analytics"
      table="user_sessions"
    />
  </Pipeline>
);
