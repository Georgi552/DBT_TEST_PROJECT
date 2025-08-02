{{ config(
    materialized='table',
    schema='REPORTING',
    tags=['REPORTING']
) }}

SELECT 
    1 as test_id,
    'staging_test' as test_name,
    CURRENT_TIMESTAMP() as created_at