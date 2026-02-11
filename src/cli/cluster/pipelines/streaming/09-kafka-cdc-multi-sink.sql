-- Debezium CDC → STATEMENT SET 3-way fan-out
-- Exercises: CDC with STATEMENT SET, multi-sink from single source, complex DAG

CREATE TABLE `cdc_products` (
  `id` INT,
  `name` STRING,
  `category` STRING,
  `price` DOUBLE,
  `quantity` INT
) WITH (
  'connector' = 'kafka',
  'topic' = 'cdc.inventory.products',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-cdc-multi-sink',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'debezium-json'
);

CREATE TABLE `product_mirror` (
  `id` INT,
  `name` STRING,
  `category` STRING,
  `price` DOUBLE,
  `quantity` INT
) WITH (
  'connector' = 'blackhole'
);

CREATE TABLE `low_stock_alerts` (
  `id` INT,
  `name` STRING,
  `category` STRING,
  `quantity` INT
) WITH (
  'connector' = 'print'
);

CREATE TABLE `premium_products` (
  `id` INT,
  `name` STRING,
  `price` DOUBLE
) WITH (
  'connector' = 'blackhole'
);

EXECUTE STATEMENT SET
BEGIN
  INSERT INTO `product_mirror`
  SELECT `id`, `name`, `category`, `price`, `quantity`
  FROM `cdc_products`;

  INSERT INTO `low_stock_alerts`
  SELECT `id`, `name`, `category`, `quantity`
  FROM `cdc_products`
  WHERE `quantity` < 50;

  INSERT INTO `premium_products`
  SELECT `id`, `name`, `price`
  FROM `cdc_products`
  WHERE `price` >= 200;
END;
