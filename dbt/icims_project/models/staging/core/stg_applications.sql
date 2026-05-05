{{ config(
    materialized='incremental',
    unique_key='application_id'
) }}

WITH source AS (

    SELECT *
    FROM {{ source('raw', 'applications') }}

    -- batch filtering
    WHERE DATE(_ingestion_ts) = '{{ var("run_date") }}'

),

deduped AS (

    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY application_id
               ORDER BY _ingestion_ts DESC
           ) AS rn
    FROM source

)

SELECT
    application_id,
    job_id,
    candidate_id,

    -- date normalization
    {{ parse_date('apply_date') }} AS apply_date,

    -- metadata
    _ingestion_ts,
    _batch_id,
    CURRENT_TIMESTAMP AS _processed_ts

FROM deduped
WHERE rn = 1