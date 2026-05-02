{{ config(materialized='table') }}

SELECT
    job_id,
    title,
    department,
    posted_date,
    status
FROM {{ ref('int_jobs_validated') }}
WHERE is_valid_job_id
  AND title IS NOT NULL
  AND posted_date IS NOT NULL
  AND status IS NOT NULL