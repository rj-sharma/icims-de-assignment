SELECT *
FROM {{ ref('stg_applications') }}
WHERE apply_date IS NULL