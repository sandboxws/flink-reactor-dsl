-- IoT device registry CDC sync
-- Exercises: Kafka CDC source, Debezium retract mode, device lifecycle tracking

CREATE TABLE `iot_devices` (
  `device_id` STRING,
  `device_name` STRING,
  `device_type` STRING,
  `location` STRING,
  `firmware` STRING,
  `status` STRING,
  `proc_time` AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'iot.registry.devices',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-iot-device-sync',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'debezium-json'
);

CREATE TABLE `device_registry_sink` (
  `device_id` STRING,
  `device_name` STRING,
  `device_type` STRING,
  `location` STRING,
  `firmware` STRING,
  `status` STRING
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO `device_registry_sink`
SELECT
  `device_id`,
  `device_name`,
  `device_type`,
  `location`,
  `firmware`,
  `status`
FROM `iot_devices`;
