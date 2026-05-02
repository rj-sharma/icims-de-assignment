WITH edu AS (
    SELECT
        candidate_id,
        list(
            JSON_OBJECT(
                'degree', degree,
                'institution', institution,
                'year', graduation_year
            )
        ) AS education
    FROM {{ ref('stg_educations') }}
    GROUP BY candidate_id
)

SELECT
    c.candidate_id,
    c.first_name,
    c.last_name,
    c.email,
    c.phone,
    c.skills,
    e.education
FROM {{ ref('stg_candidates') }} c
LEFT JOIN edu e
    ON c.candidate_id = e.candidate_id