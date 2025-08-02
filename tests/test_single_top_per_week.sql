WITH counts AS (
  SELECT
    year,
    week_NUMBEr,
    COUNT_IF(IS_WEEK_TOP_SELER = 1) AS num_top
    FROM georgi_hw.georgi_hw_reporting.WEEKLY_TOP_SALERS
  -- FROM {{ ref('vw_weekly_product_sales') }}
  GROUP BY all
)
SELECT *
FROM counts
WHERE num_top <> 1