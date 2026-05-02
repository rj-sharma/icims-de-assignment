SELECT
    TRIM(candidate_id) AS candidate_id,
    TRIM(degree) AS degree,
    TRIM(institution) AS institution,
    CAST(year AS INTEGER) AS graduation_year
FROM {{ source('raw', 'education') }}
WHERE candidate_id IS NOT NULL