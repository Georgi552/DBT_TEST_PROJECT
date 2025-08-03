# dbt_snowflake_project

> A dbt + Snowflake analytics pipeline for Fastmarkets interview

## Overview

This project demonstrates a full end-to-end analytics workflow:

1. **Data Ingestion**  
   A Python script downloads the raw CSV from GitHub and loads it into Snowflake.
2. **dbt Transformations**  
   dbt models in dbt Cloud clean, normalize, and reshape the data into staging, transformation, and reporting schemas.
3. **Weekly Reporting**  
   A reporting view calculates weekly product‐level sales and flags the top seller each week over the last three years.
4. **Automated Testing**  
   Generic and custom SQL tests validate data availability, uniqueness, referential integrity, and business logic.
5. **Scheduling**  
   dbt jobs in dbt Cloud run at 12:00 UTC and 18:00 UTC daily.
6. **Results**  
   All tables and views live in Snowflake so stakeholders can query them directly.
7. **Results visualisation**
   Streamlit app for viewing results: https://app.snowflake.com/rzkynof/he73735/#/streamlit-apps/GEORGI_HW.GEORGI_HW_REPORTING.FEQSQY7HETS08_SK?ref=snowsight_shared
   <img width="1808" height="951" alt="image" src="https://github.com/user-attachments/assets/3d190c19-b1de-42d4-af58-67ccef0c64b2" />


---

## Architecture
<img width="1344" height="608" alt="image" src="https://github.com/user-attachments/assets/4bfca7bf-d5fe-4da5-980b-c694704945be" />

## SCHEDULE
<img width="1623" height="375" alt="image" src="https://github.com/user-attachments/assets/9d90f6a2-f5a3-47c9-8de3-9f7c0a026e66" />

