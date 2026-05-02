SELECT DISTINCT
    TRIM(job_id) AS job_id,
    TRIM(title) AS title,
    TRIM(department) AS department,
    CAST(posted_date AS DATE) AS posted_date,
    LOWER(TRIM(status)) AS status
FROM {{ source('raw', 'jobs') }}
WHERE job_id IS NOT NULL