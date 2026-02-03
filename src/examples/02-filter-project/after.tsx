import { createElement } from '../../core/jsx-runtime';
import { Schema, Field } from '../../core/schema';
import { Pipeline } from '../../components/pipeline';
import { KafkaSource } from '../../components/sources';
import { GenericSink } from '../../components/sinks';
import { Filter, Map } from '../../components/transforms';

const OrderSchema = Schema({
  fields: {
    order_id: Field.BIGINT(),
    user_id: Field.STRING(),
    product_id: Field.STRING(),
    amount: Field.DECIMAL(10, 2),
    order_time: Field.TIMESTAMP(3),
  },
});

export default (
  <Pipeline name="filter-project" parallelism={8}>
    <KafkaSource
      topic="orders"
      bootstrapServers="kafka:9092"
      schema={OrderSchema}
    />
    <Filter condition="amount > 100" />
    <Map select={{
      order_id: 'order_id',
      user_id: 'user_id',
      amount: 'amount',
      order_time: 'order_time',
    }} />
    <GenericSink connector="print" />
  </Pipeline>
);
