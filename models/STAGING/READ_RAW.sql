{{ config(
    materialized='table',
    schema='STAGING',
    tags=['STAGING']
) }}

SELECT * from GEORGI_HW_RAW.PYTHON_IMPORT.HOMEWORK_RAW