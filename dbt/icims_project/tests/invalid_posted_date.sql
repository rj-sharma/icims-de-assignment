SELECT *
FROM {{ ref('stg_jobs') }}
WHERE posted_date IS NULL