SELECT DISTINCT
    job_id,
    title,
    department,
    posted_date,
    status
FROM {{ ref('stg_jobs') }}