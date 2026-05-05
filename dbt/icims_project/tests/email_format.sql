SELECT *
FROM {{ ref('stg_candidates') }}
WHERE email NOT LIKE '%@%.%'