-- IoT device fleet summary: count by type and status
-- Exercises: batch mode, datagen source, GROUP BY aggregate, device inventory

SET 'execution.runtime-mode' = 'batch';

CREATE TABLE `devices` (
  `device_id` STRING,
  `device_type` STRING,
  `status` STRING,
  `location` STRING,
  `firmware` STRING
) WITH (
  'connector' = 'datagen',
  'number-of-rows' = '1000',
  'fields.device_id.length' = '7',
  'fields.device_type.length' = '5',
  'fields.status.length' = '5',
  'fields.location.length' = '8',
  'fields.firmware.length' = '5'
);

CREATE TABLE `device_summary` (
  `device_type` STRING,
  `status` STRING,
  `device_count` BIGINT
) WITH (
  'connector' = 'print'
);

INSERT INTO `device_summary`
SELECT
  `device_type`,
  `status`,
  COUNT(*) AS `device_count`
FROM `devices`
GROUP BY `device_type`, `status`;
