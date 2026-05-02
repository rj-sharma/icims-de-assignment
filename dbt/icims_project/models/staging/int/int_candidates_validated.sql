{{ config(materialized='table') }}

WITH base AS (
    SELECT
        NULLIF(TRIM(candidate_id), '') AS candidate_id,
        NULLIF(TRIM(first_name), '') AS first_name,
        NULLIF(TRIM(last_name), '') AS last_name,
        NULLIF(LOWER(TRIM(email)), '') AS email,
        NULLIF(TRIM(phone), '') AS phone,
        skills
    FROM {{ source('raw', 'candidates') }}
)
SELECT
    candidate_id,
    first_name,
    last_name,
    email,
    phone,
    skills,
    REGEXP_MATCHES(
        LOWER(candidate_id),
        '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
    ) AS is_valid_candidate_id,
    email IS NOT NULL
    AND REGEXP_MATCHES(
        email,
        '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
    ) AS is_valid_email
FROM base
