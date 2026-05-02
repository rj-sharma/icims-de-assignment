{{ config(materialized='table') }}

WITH base AS (
    SELECT
        NULLIF(TRIM(job_id), '') AS job_id,
        NULLIF(TRIM(title), '') AS title,
        NULLIF(TRIM(department), '') AS department,
        NULLIF(TRIM(posted_date), '') AS posted_date_raw,
        NULLIF(LOWER(TRIM(status)), '') AS status
    FROM {{ source('raw', 'jobs') }}
)
SELECT
    job_id,
    title,
    department,
    posted_date_raw,
    COALESCE(
        TRY_STRPTIME(posted_date_raw, '%Y-%m-%d'),
        TRY_STRPTIME(posted_date_raw, '%Y.%m.%d'),
        TRY_STRPTIME(posted_date_raw, '%d-%b-%Y'),
        TRY_STRPTIME(posted_date_raw, '%B %d, %Y'),
        TRY_STRPTIME(posted_date_raw, '%b %d, %Y'),
        TRY_STRPTIME(posted_date_raw, '%Y/%m/%d')
    )::DATE AS posted_date,
    status,
    REGEXP_MATCHES(
        LOWER(job_id),
        '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
    ) AS is_valid_job_id
FROM base
