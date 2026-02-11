-- Continuous Top-N leaderboard
-- Exercises: Top-N pattern with ROW_NUMBER, retract stream, state updates

CREATE TABLE `scores` (
  `player_id` INT,
  `game` STRING,
  `score` INT,
  `play_time` TIMESTAMP(3),
  WATERMARK FOR `play_time` AS `play_time` - INTERVAL '5' SECOND
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '30',
  'fields.player_id.min' = '1',
  'fields.player_id.max' = '100',
  'fields.game.length' = '6',
  'fields.score.min' = '0',
  'fields.score.max' = '10000'
);

CREATE TABLE `leaderboard` (
  `game` STRING,
  `player_id` INT,
  `top_score` INT,
  `rank_num` BIGINT
) WITH (
  'connector' = 'print'
);

INSERT INTO `leaderboard`
SELECT `game`, `player_id`, `top_score`, `rank_num`
FROM (
  SELECT
    `game`,
    `player_id`,
    MAX(`score`) AS `top_score`,
    ROW_NUMBER() OVER (PARTITION BY `game` ORDER BY MAX(`score`) DESC) AS `rank_num`
  FROM `scores`
  GROUP BY `game`, `player_id`
)
WHERE `rank_num` <= 10;
