{{ config(materialized='table') }}

SELECT
    candidate_id,
    degree,
    institution,
    graduation_year
FROM {{ ref('int_educations_validated') }}
WHERE is_valid_candidate_id
  AND degree IS NOT NULL
  AND institution IS NOT NULL
  AND graduation_year IS NOT NULL