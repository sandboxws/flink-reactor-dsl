-- IoT alert routing: high temp (>85) and low humidity (<20)
-- Exercises: Kafka CDC source, filter conditions, STATEMENT SET, alert routing

CREATE TABLE `iot_telemetry` (
  `device_id` STRING,
  `sensor_type` STRING,
  `value` DOUBLE,
  `unit` STRING,
  `reading_time` TIMESTAMP(3),
  `location` STRING,
  WATERMARK FOR `reading_time` AS `reading_time` - INTERVAL '30' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'iot.telemetry.readings',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-iot-alerts',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'debezium-json'
);

CREATE TABLE `high_temp_alerts` (
  `device_id` STRING,
  `location` STRING,
  `value` DOUBLE,
  `reading_time` TIMESTAMP(3)
) WITH (
  'connector' = 'print'
);

CREATE TABLE `low_humidity_alerts` (
  `device_id` STRING,
  `location` STRING,
  `value` DOUBLE,
  `reading_time` TIMESTAMP(3)
) WITH (
  'connector' = 'print'
);

EXECUTE STATEMENT SET
BEGIN
  INSERT INTO `high_temp_alerts`
  SELECT `device_id`, `location`, `value`, `reading_time`
  FROM `iot_telemetry`
  WHERE `sensor_type` = 'temperature' AND `value` > 85;

  INSERT INTO `low_humidity_alerts`
  SELECT `device_id`, `location`, `value`, `reading_time`
  FROM `iot_telemetry`
  WHERE `sensor_type` = 'humidity' AND `value` < 20;
END;
