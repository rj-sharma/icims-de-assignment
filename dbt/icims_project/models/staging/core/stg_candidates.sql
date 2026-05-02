{{ config(materialized='table') }}

SELECT
    candidate_id,
    first_name,
    last_name,
    email,
    phone,
    skills
FROM {{ ref('int_candidates_validated') }}
WHERE is_valid_candidate_id
  AND is_valid_email