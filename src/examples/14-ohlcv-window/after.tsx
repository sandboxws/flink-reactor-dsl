import { createElement } from '../../core/jsx-runtime';
import { Schema, Field } from '../../core/schema';
import { Pipeline } from '../../components/pipeline';
import { KafkaSource } from '../../components/sources';
import { KafkaSink } from '../../components/sinks';
import { Aggregate } from '../../components/transforms';
import { TumbleWindow } from '../../components/windows';

const TradeSchema = Schema({
  fields: {
    symbol: Field.STRING(),
    price: Field.DECIMAL(12, 4),
    volume: Field.BIGINT(),
    trade_time: Field.TIMESTAMP(3),
  },
  watermark: {
    column: 'trade_time',
    expression: "trade_time - INTERVAL '10' SECOND",
  },
});

export default (
  <Pipeline name="ohlcv-1min">
    <KafkaSource
      topic="trades"
      bootstrapServers="kafka:9092"
      schema={TradeSchema}
    />
    <TumbleWindow size="1 minute" on="trade_time">
      <Aggregate
        groupBy={['symbol']}
        select={{
          symbol: 'symbol',
          open_price: 'FIRST_VALUE(price)',
          high_price: 'MAX(price)',
          low_price: 'MIN(price)',
          close_price: 'LAST_VALUE(price)',
          total_volume: 'SUM(volume)',
        }}
      />
    </TumbleWindow>
    <KafkaSink topic="ohlcv_1min" />
  </Pipeline>
);
