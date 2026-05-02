SELECT DISTINCT
    TRIM(application_id) AS application_id,
    TRIM(job_id) AS job_id,
    TRIM(candidate_id) AS candidate_id,
    CAST(apply_date AS DATE) AS apply_date
FROM {{ source('raw', 'applications') }}
WHERE application_id IS NOT NULL