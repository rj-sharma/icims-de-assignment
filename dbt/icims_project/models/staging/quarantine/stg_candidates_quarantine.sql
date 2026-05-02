{{ config(materialized='table') }}

SELECT
    candidate_id,
    first_name,
    last_name,
    email,
    phone,
    skills,
    CASE
        WHEN NOT is_valid_candidate_id THEN 'INVALID_CANDIDATE_ID'
        WHEN NOT is_valid_email THEN 'INVALID_EMAIL'
        ELSE 'UNKNOWN'
    END AS quarantine_reason
FROM {{ ref('int_candidates_validated') }}
WHERE NOT is_valid_candidate_id
   OR NOT is_valid_email
