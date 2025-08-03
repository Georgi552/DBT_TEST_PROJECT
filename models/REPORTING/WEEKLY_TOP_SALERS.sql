{{ config(
    materialized='view',
    schema='REPORTING',
    tags=['REPORTING']
) }}

with orders as (
    select * from {{ ref('FACT_ORDERS') }} 
)

, aggregated as (
    select 
        YEAR,
        WEEK_NUMBER,  
        PRODUCT_ID,
        PRODUCT_NAME,
        sum(QUANTITY) as total_sales
    
    from orders
    group by all
)
, ranked AS (
  SELECT
    year,
    WEEK_NUMBER,
    PRODUCT_ID,
    PRODUCT_NAME,
    total_sales,
    ROW_NUMBER() OVER (
      PARTITION BY year, WEEK_NUMBER
      ORDER BY total_sales DESC
    )                                           AS rank_in_week,

    CASE 
        WHEN rank_in_week = 1 THEN '1'
        ELSE '0'
    END as IS_WEEK_TOP_SELER
  FROM aggregated
)

select * from ranked
order by year, week_number