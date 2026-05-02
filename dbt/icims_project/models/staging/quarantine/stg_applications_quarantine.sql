{{ config(materialized='table') }}

SELECT
    application_id,
    job_id,
    candidate_id,
    apply_date_raw,
    apply_date,
    CASE
        WHEN NOT is_valid_application_id THEN 'INVALID_APPLICATION_ID'
        WHEN NOT is_valid_job_id THEN 'INVALID_JOB_ID'
        WHEN NOT is_valid_candidate_id THEN 'INVALID_CANDIDATE_ID'
        WHEN apply_date IS NULL THEN 'INVALID_APPLY_DATE'
        ELSE 'UNKNOWN'
    END AS quarantine_reason
FROM {{ ref('int_applications_validated') }}
WHERE NOT is_valid_application_id
   OR NOT is_valid_job_id
   OR NOT is_valid_candidate_id
   OR apply_date IS NULL
