{{ config(
    materialized='incremental',
    unique_key='job_id'
) }}

WITH source AS (

    SELECT *
    FROM {{ source('raw', 'jobs') }}
    WHERE DATE(_ingestion_ts) = '{{ var("run_date") }}'

),

deduped AS (

    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY job_id
               ORDER BY _ingestion_ts DESC
           ) AS rn
    FROM source

),

cleaned AS (

    SELECT
        job_id,

        -- standardization
        TRIM(title) AS title,
        UPPER(TRIM(department)) AS department,
        UPPER(TRIM(status)) AS status,

        -- reuse macro (important)
        {{ parse_date('posted_date') }} AS posted_date,

        _ingestion_ts,
        _batch_id

    FROM deduped
    WHERE rn = 1

)

SELECT
    *,
    CURRENT_TIMESTAMP AS _processed_ts
FROM cleaned