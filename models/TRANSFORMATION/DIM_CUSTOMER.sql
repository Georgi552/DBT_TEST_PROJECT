{{ config(
    materialized='table',
    schema='TRANSFORMATION',
    tags=['TRANSFORMATION', 'DIM']
) }}


SELECT DISTINCT
  CAST(CUSTOMER_ID as NUMBER)      AS customer_id,
  CUSTOMER_NAME                    AS CUSTOMER_NAME,
  lower(CUSTOMER_EMAIL)            AS CUSTOMER_EMAIL,
  customer_phone,
  REGEXP_REPLACE(CUSTOMER_PHONE, '[^0-9]', '') AS phone_clean

FROM {{ ref('READ_RAW') }}
order by customer_id
