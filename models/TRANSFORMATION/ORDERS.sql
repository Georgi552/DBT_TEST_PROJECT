{{ config(
    materialized='table',
    schema='TRANSFORMATION',
    tags=['TRANSFORMATION', 'DIM']
) }}



SELECT 

  o.ORDER_ID                                     AS ORDER_ID,
  o.CUSTOMER_ID                                  AS CUSTOMER_ID,
  TO_DATE(o.ORDER_DATE)                          AS order_date,
  
    f.value:product_id::STRING     AS product_id,
    f.value:product_name::STRING   AS product_name,
    f.value:quantity::NUMBER       AS quantity,
    f.value:price::NUMBER          AS price
FROM {{ ref('READ_RAW') }} o,
LATERAL FLATTEN(INPUT => PARSE_JSON(o.ORDER_ITEMS)) f


