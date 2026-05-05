{{ config(
    materialized='incremental',
    unique_key='candidate_id',
    tags=['stg']
) }}

WITH source AS (

    SELECT *
    FROM {{ source('raw', 'candidates') }}
    WHERE DATE(_ingestion_ts) = '{{ var("run_date") }}'

),

deduped AS (

    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY candidate_id
               ORDER BY _ingestion_ts DESC
           ) AS rn
    FROM source

),

cleaned AS (

    SELECT
        candidate_id,

        -- standardization
        TRIM(first_name) AS first_name,
        TRIM(last_name) AS last_name,
        LOWER(TRIM(email)) AS email,
        TRIM(phone) AS phone,

        skills,

        -- optional enrichment (nice signal)
        CASE 
            WHEN skills IS NOT NULL THEN ARRAY_LENGTH(STRING_SPLIT(skills, ','))
            ELSE 0
        END AS skills_count,

        _ingestion_ts,
        _batch_id

    FROM deduped
    WHERE rn = 1

)

SELECT
    *,
    CURRENT_TIMESTAMP AS _processed_ts
FROM cleaned