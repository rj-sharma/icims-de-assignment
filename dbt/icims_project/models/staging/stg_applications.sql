{{ config(materialized='table') }}

SELECT
    application_id,
    job_id,
    candidate_id,
    apply_date
FROM {{ ref('int_applications_validated') }}
WHERE is_valid_application_id
  AND is_valid_job_id
  AND is_valid_candidate_id
  AND apply_date IS NOT NULL