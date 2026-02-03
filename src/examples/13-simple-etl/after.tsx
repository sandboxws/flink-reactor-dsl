import { createElement } from '../../core/jsx-runtime';
import { Schema, Field } from '../../core/schema';
import { Pipeline } from '../../components/pipeline';
import { KafkaSource } from '../../components/sources';
import { GenericSink } from '../../components/sinks';
import { Filter } from '../../components/transforms';

const LogSchema = Schema({
  fields: {
    log_id: Field.STRING(),
    timestamp: Field.TIMESTAMP(3),
    level: Field.STRING(),
    message: Field.STRING(),
    service_name: Field.STRING(),
  },
});

export default (
  <Pipeline name="error-log-etl">
    <KafkaSource
      topic="raw_logs"
      bootstrapServers="kafka:9092"
      schema={LogSchema}
    />
    <Filter condition="level IN ('ERROR', 'WARN')" />
    <GenericSink
      connector="elasticsearch-7"
      options={{
        'hosts': 'http://elasticsearch:9200',
        'index': 'error_logs',
      }}
    />
  </Pipeline>
);
