-- OHLCV Trade Aggregation
-- Computes Open-High-Low-Close-Volume candlestick data
-- per symbol in 1-minute tumbling windows.

CREATE TABLE `trades` (
  `symbol` STRING,
  `price` DECIMAL(12, 4),
  `volume` BIGINT,
  `trade_time` TIMESTAMP(3),
  WATERMARK FOR `trade_time` AS `trade_time` - INTERVAL '10' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'trades',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

CREATE TABLE `ohlcv_1min` (
  `symbol` STRING,
  `open_price` DECIMAL(12, 4),
  `high_price` DECIMAL(12, 4),
  `low_price` DECIMAL(12, 4),
  `close_price` DECIMAL(12, 4),
  `total_volume` BIGINT,
  `window_end` TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'ohlcv_1min',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

INSERT INTO `ohlcv_1min`
SELECT
  `symbol`,
  FIRST_VALUE(`price`) AS `open_price`,
  MAX(`price`) AS `high_price`,
  MIN(`price`) AS `low_price`,
  LAST_VALUE(`price`) AS `close_price`,
  SUM(`volume`) AS `total_volume`,
  `window_end`
FROM TABLE(
  TUMBLE(TABLE `trades`, DESCRIPTOR(`trade_time`), INTERVAL '1' MINUTE)
)
GROUP BY `symbol`, `window_start`, `window_end`;
