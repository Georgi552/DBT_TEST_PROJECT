{{ config(
    materialized='table',
    schema='TRANSFORMATION',
    tags=['TRANSFORMATION', 'DIM']
) }}

WITH bounds AS (
  SELECT
    MIN(TO_DATE(order_date)) AS start_date,
    MAX(TO_DATE(order_date)) AS end_date
    FROM {{ ref('READ_RAW') }}
),
diff AS (
  SELECT
    start_date,
    end_date,
    DATEDIFF('day', start_date, end_date) AS diff_days
  FROM bounds
),
all_dates AS (
  SELECT
    DATEADD('day', SEQ4(), d.start_date) AS activity_date
  FROM diff d
  JOIN TABLE(GENERATOR(ROWCOUNT => 10000)) g  -- pick a number >= max possible span
    ON SEQ4() <= d.diff_days

)
,final as (
SELECT
  activity_date                           AS activity_date,
  EXTRACT(YEAR  FROM activity_date)           AS year,
  EXTRACT(MONTH FROM activity_date)           AS month,
  EXTRACT(DAY   FROM activity_date)           AS day_of_month,
  DATE_TRUNC('WEEK', activity_date)           AS week_start,
  DATEADD(DAY, 6, DATE_TRUNC('WEEK', activity_date)) AS week_end,
  QUARTER(activity_date)                      AS quarter,
  WEEK(activity_date)                         AS week_number
  
  -- CASE
  --   WHEN MONTH IN (12,1,2) THEN 'Winter'
  --   WHEN MONTH IN (3,4,5)  THEN 'Spring'
  --   WHEN MONTH IN (6,7,8)  THEN 'Summer'
  --   ELSE 'Autumn'
  -- END                                      AS season
FROM all_dates
)
select * from final 