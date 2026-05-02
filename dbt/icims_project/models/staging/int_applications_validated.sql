{{ config(materialized='table') }}

WITH base AS (
    SELECT
        NULLIF(TRIM(application_id), '') AS application_id,
        NULLIF(TRIM(job_id), '') AS job_id,
        NULLIF(TRIM(candidate_id), '') AS candidate_id,
        NULLIF(TRIM(apply_date), '') AS apply_date_raw
    FROM {{ source('raw', 'applications') }}
)
SELECT
    application_id,
    job_id,
    candidate_id,
    apply_date_raw,
    -- This can be moved to Python ingestion later for richer date parsing.
    COALESCE(
        TRY_STRPTIME(apply_date_raw, '%Y-%m-%d'),
        TRY_STRPTIME(apply_date_raw, '%Y.%m.%d'),
        TRY_STRPTIME(apply_date_raw, '%d-%b-%Y'),
        TRY_STRPTIME(apply_date_raw, '%B %d, %Y'),
        TRY_STRPTIME(apply_date_raw, '%b %d, %Y'),
        TRY_STRPTIME(apply_date_raw, '%Y/%m/%d')
    )::DATE AS apply_date,
    application_id IS NOT NULL AS is_valid_application_id,
    job_id IS NOT NULL AS is_valid_job_id,
    candidate_id IS NOT NULL AS is_valid_candidate_id
FROM base
