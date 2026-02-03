-- Simple Source to Sink
-- A basic Kafka passthrough pipeline that reads user events
-- and writes them to a processed topic.

CREATE TABLE `user_events` (
  `event_id` STRING,
  `user_id` STRING,
  `event_type` STRING,
  `payload` STRING,
  `event_time` TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'user_events',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json',
  'scan.startup.mode' = 'latest-offset'
);

CREATE TABLE `user_events_processed` (
  `event_id` STRING,
  `user_id` STRING,
  `event_type` STRING,
  `payload` STRING,
  `event_time` TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'user_events_processed',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

INSERT INTO `user_events_processed`
SELECT * FROM `user_events`;
