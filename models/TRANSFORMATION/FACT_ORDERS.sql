{{ config(
    materialized='table',
    schema='TRANSFORMATION',
    tags=['TRANSFORMATION']
) }}

with orders as (
    select * from {{ ref('ORDERS') }}
)
, dim_date as (
    select * from {{ ref('DIM_DATE') }}
)
, dim_customer as (
    select * from {{ ref('DIM_CUSTOMER') }}
)
, dim_product as (
    select * from {{ ref('DIM_PRODUCT') }}
)

select 
    ord.ORDER_ID, 
    ord.CUSTOMER_ID, 
    ord.ORDER_DATE, 
    ord.PRODUCT_ID, 
    ord.PRODUCT_NAME, 
    ord.QUANTITY, 
    ord.PRICE,
    dim_date.year, 
    dim_date.week_number,
    dim_date.day_of_month,
    dim_date.month,
    dim_date.quarter,
    dim_cust.customer_name,
    dim_cust.customer_email,
    dim_cust.customer_phone,
    dim_prod.product_name as product_name_dim,
    dim_prod.price as price_dim
    
from orders ord
left join DIM_DATE dim_date
on dim_date.activity_date = ord.order_date

left join dim_customer dim_cust
    on dim_cust.customer_id = ord.customer_id

left join dim_product dim_prod
    on dim_prod.product_id = ord.product_id
