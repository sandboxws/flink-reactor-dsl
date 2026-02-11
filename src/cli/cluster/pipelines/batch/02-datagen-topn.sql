-- Bounded datagen → ranking
-- Exercises: batch Top-N, window ranking, FINISHED job state

SET 'execution.runtime-mode' = 'batch';

CREATE TABLE `employees` (
  `dept_id` INT,
  `employee_id` INT,
  `salary` DOUBLE,
  `hire_date` STRING
) WITH (
  'connector' = 'datagen',
  'number-of-rows' = '5000',
  'fields.dept_id.min' = '1',
  'fields.dept_id.max' = '10',
  'fields.employee_id.min' = '1',
  'fields.employee_id.max' = '5000',
  'fields.salary.min' = '30000',
  'fields.salary.max' = '200000',
  'fields.hire_date.length' = '10'
);

CREATE TABLE `top_earners` (
  `dept_id` INT,
  `employee_id` INT,
  `salary` DOUBLE,
  `rank_num` BIGINT
) WITH (
  'connector' = 'print'
);

INSERT INTO `top_earners`
SELECT `dept_id`, `employee_id`, `salary`, `rank_num`
FROM (
  SELECT
    `dept_id`,
    `employee_id`,
    `salary`,
    ROW_NUMBER() OVER (PARTITION BY `dept_id` ORDER BY `salary` DESC) AS `rank_num`
  FROM `employees`
)
WHERE `rank_num` <= 5;
