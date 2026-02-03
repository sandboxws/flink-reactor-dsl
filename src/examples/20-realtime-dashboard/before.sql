-- Realtime Dashboard: Top Categories per Region
-- Enriches sales with product + store dimensions via lookup joins,
-- aggregates revenue per region/category in 1-minute windows,
-- then ranks top 5 categories per region.

CREATE TABLE `sales_transactions` (
  `transaction_id` STRING,
  `store_id` STRING,
  `product_id` STRING,
  `quantity` INT,
  `unit_price` DECIMAL(10, 2),
  `transaction_time` TIMESTAMP(3),
  WATERMARK FOR `transaction_time` AS `transaction_time` - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'sales_transactions',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

CREATE TABLE `dim_products` (
  `product_id` STRING,
  `product_name` STRING,
  `category` STRING,
  `brand` STRING,
  PRIMARY KEY (`product_id`) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://db:3306/catalog',
  'table-name' = 'dim_products',
  'driver' = 'com.mysql.cj.jdbc.Driver',
  'lookup.cache' = 'LRU',
  'lookup.cache.max-rows' = '50000',
  'lookup.cache.ttl' = '300s'
);

CREATE TABLE `dim_stores` (
  `store_id` STRING,
  `store_name` STRING,
  `region` STRING,
  `city` STRING,
  PRIMARY KEY (`store_id`) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://db:3306/catalog',
  'table-name' = 'dim_stores',
  'driver' = 'com.mysql.cj.jdbc.Driver',
  'lookup.cache' = 'LRU',
  'lookup.cache.max-rows' = '10000',
  'lookup.cache.ttl' = '600s'
);

CREATE TABLE `dashboard_top_categories_per_region` (
  `region` STRING,
  `category` STRING,
  `revenue` DECIMAL(10, 2),
  `units_sold` BIGINT,
  `transaction_count` BIGINT,
  `window_start` TIMESTAMP(3),
  `window_end` TIMESTAMP(3),
  `rank_num` BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'dashboard_top_categories_per_region',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

-- Lookup-enrich, window-aggregate, then rank top 5
INSERT INTO `dashboard_top_categories_per_region`
SELECT
  `region`, `category`, `revenue`, `units_sold`,
  `transaction_count`, `window_start`, `window_end`, `rank_num`
FROM (
  SELECT
    `region`, `category`, `revenue`, `units_sold`,
    `transaction_count`, `window_start`, `window_end`,
    ROW_NUMBER() OVER (
      PARTITION BY `window_start`, `region`
      ORDER BY `revenue` DESC
    ) AS `rank_num`
  FROM (
    SELECT
      s.`region`,
      p.`category`,
      SUM(t.`quantity` * t.`unit_price`) AS `revenue`,
      SUM(t.`quantity`) AS `units_sold`,
      COUNT(*) AS `transaction_count`,
      `window_start`,
      `window_end`
    FROM TABLE(
      TUMBLE(TABLE `sales_transactions`, DESCRIPTOR(`transaction_time`), INTERVAL '1' MINUTE)
    ) AS t
    JOIN `dim_products` FOR SYSTEM_TIME AS OF t.`proc_time` AS p
      ON t.`product_id` = p.`product_id`
    JOIN `dim_stores` FOR SYSTEM_TIME AS OF t.`proc_time` AS s
      ON t.`store_id` = s.`store_id`
    GROUP BY s.`region`, p.`category`, `window_start`, `window_end`
  )
)
WHERE `rank_num` <= 5;
