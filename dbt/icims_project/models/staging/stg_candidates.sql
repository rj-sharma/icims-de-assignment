SELECT DISTINCT
    TRIM(candidate_id) AS candidate_id,
    TRIM(first_name) AS first_name,
    TRIM(last_name) AS last_name,
    LOWER(TRIM(email)) AS email,
    TRIM(phone) AS phone,
    skills
FROM {{ source('raw', 'candidates') }}
WHERE candidate_id IS NOT NULL