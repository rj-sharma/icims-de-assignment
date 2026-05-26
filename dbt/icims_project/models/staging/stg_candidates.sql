{{ config(
    materialized='incremental',
    unique_key='candidate_id',
    tags=['stg']
) }}

WITH source AS (

    SELECT *
    FROM {{ source('raw', 'candidates') }}
    WHERE _ingestion_date = CAST('{{ var("run_date") }}' AS DATE)

),

normalized AS (

    SELECT
        *,
        list_sort(
            list_transform(
                string_split(skills, ','),
                skill -> TRIM(skill)
            )
        ) AS skills_array
    FROM source

),

deduped AS (

    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY candidate_id
               ORDER BY _ingestion_ts DESC
           ) AS rn
    FROM normalized

),

cleaned AS (

    SELECT
        candidate_id,

        -- standardization
        TRIM(first_name) AS first_name,
        TRIM(last_name) AS last_name,
        LOWER(TRIM(email)) AS email,
        email_hash,
        TRIM(phone) AS phone,
        phone_hash,

        skills,
        skills_array,
        array_to_string(skills_array, ',') AS skills_normalized,

        --Enrichment 
        CASE 
            WHEN skills IS NOT NULL THEN ARRAY_LENGTH(skills_array)
            ELSE 0
        END AS skills_count,

        _ingestion_ts,
        _ingestion_date,
        _batch_id,
        _source_system,
        _file_name,
        _source_file_checksum,
        _record_hash

    FROM deduped
    WHERE rn = 1

)

SELECT
    *,
    CURRENT_TIMESTAMP AS _processed_ts
FROM cleaned
