# dbt_test_project

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
   All artifacts—tables, views, docs—live in Snowflake so stakeholders can query them directly.

---

## Architecture
<img width="1344" height="608" alt="image" src="https://github.com/user-attachments/assets/4bfca7bf-d5fe-4da5-980b-c694704945be" />
