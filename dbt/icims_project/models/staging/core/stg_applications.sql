{{ config(
    materialized='incremental',
    unique_key='application_id',
    tags=['stg']
) }}

WITH source AS (

    SELECT *
    FROM {{ source('raw', 'applications') }}

    WHERE _ingestion_date = CAST('{{ var("run_date") }}' AS DATE)

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
    _ingestion_date,
    _source_system,
    _file_name,
    _source_file_checksum,
    _record_hash,
    CURRENT_TIMESTAMP AS _processed_ts

FROM deduped
WHERE rn = 1
