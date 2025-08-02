{{ config(
    materialized='table',
    schema='TRANSFORMATION',
    tags=['TRANSFORMATION', 'DIM']
) }}


WITH source AS (
  SELECT order_id,
         order_items -- the raw string column
  FROM {{ ref('READ_RAW') }}
),

parsed AS (
  SELECT
    order_id,
    -- try to parse directly; if it's double-encoded with backslashes, undo them first
    CASE
      WHEN IS_ARRAY(TRY_PARSE_JSON(order_items)) THEN TRY_PARSE_JSON(order_items)
      ELSE TRY_PARSE_JSON(REPLACE(order_items, '\\\"', '\"'))
    END AS items_array
  FROM source
),

exploded AS (
  SELECT distinct 
    -- order_id,
    f.value:product_id::STRING   AS product_id,
    f.value:product_name::STRING AS product_name,
    -- f.value:quantity::NUMBER     AS quantity,
    f.value:price::NUMBER        AS price
  FROM parsed,
  LATERAL FLATTEN(input => items_array) f
)

SELECT *
FROM exploded
