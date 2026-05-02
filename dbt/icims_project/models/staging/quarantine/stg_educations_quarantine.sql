{{ config(materialized='table') }}

SELECT
    candidate_id,
    degree,
    institution,
    graduation_year_raw,
    graduation_year,
    CASE
        WHEN NOT is_valid_candidate_id THEN 'INVALID_CANDIDATE_ID'
        WHEN degree IS NULL THEN 'INVALID_DEGREE'
        WHEN institution IS NULL THEN 'INVALID_INSTITUTION'
        WHEN graduation_year IS NULL THEN 'INVALID_GRADUATION_YEAR'
        ELSE 'UNKNOWN'
    END AS quarantine_reason
FROM {{ ref('int_educations_validated') }}
WHERE NOT is_valid_candidate_id
   OR degree IS NULL
   OR institution IS NULL
   OR graduation_year IS NULL
