{{ config(
    materialized='incremental',
    unique_key='candidate_id',
    tags=['stg']
) }}

WITH source AS (

    SELECT *
    FROM {{ source('raw', 'education') }}
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

standardized AS (

    SELECT
        candidate_id,

        -- normalize degree (important signal)
        UPPER(TRIM(degree)) AS degree,

        TRIM(institution) AS institution,
        year,

        _ingestion_ts,
        _batch_id

    FROM deduped
    WHERE rn = 1

)

SELECT
    *,
    CURRENT_TIMESTAMP AS _processed_ts
FROM standardized