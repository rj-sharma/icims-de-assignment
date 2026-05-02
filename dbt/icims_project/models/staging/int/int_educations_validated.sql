{{ config(materialized='table') }}

WITH base AS (
    SELECT
        NULLIF(TRIM(candidate_id), '') AS candidate_id,
        NULLIF(TRIM(degree), '') AS degree,
        NULLIF(TRIM(institution), '') AS institution,
        NULLIF(TRIM(CAST(year AS VARCHAR)), '') AS graduation_year_raw
    FROM {{ source('raw', 'education') }}
)
SELECT
    candidate_id,
    degree,
    institution,
    graduation_year_raw,
    TRY_CAST(graduation_year_raw AS INTEGER) AS graduation_year,
    REGEXP_MATCHES(
        LOWER(candidate_id),
        '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
    ) AS is_valid_candidate_id
FROM base
