{{ config(materialized='table') }}

SELECT
    job_id,
    title,
    department,
    posted_date_raw,
    posted_date,
    status,
    CASE
        WHEN NOT is_valid_job_id THEN 'INVALID_JOB_ID'
        WHEN title IS NULL THEN 'INVALID_TITLE'
        WHEN posted_date IS NULL THEN 'INVALID_POSTED_DATE'
        WHEN status IS NULL THEN 'INVALID_STATUS'
        ELSE 'UNKNOWN'
    END AS quarantine_reason
FROM {{ ref('int_jobs_validated') }}
WHERE NOT is_valid_job_id
   OR title IS NULL
   OR posted_date IS NULL
   OR status IS NULL
