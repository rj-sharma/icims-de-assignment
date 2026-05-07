{{ config(
    materialized='incremental',
    unique_key='event_id',
    tags=['stg']
) }}

WITH source AS (

    SELECT *
    FROM {{ source('raw', 'workflow_events') }}
    WHERE _ingestion_date = CAST('{{ var("run_date") }}' AS DATE)

),

parsed AS (

    SELECT
        _event_id AS event_id,
        application_id,
        UPPER(TRIM(old_status)) AS old_status,
        UPPER(TRIM(new_status)) AS new_status,

        -- timestamp normalization
        TRY_CAST(event_timestamp AS TIMESTAMP) AS event_timestamp,
        CAST(TRY_CAST(event_timestamp AS TIMESTAMP) AS DATE) AS event_date,

        _ingestion_ts,
        _ingestion_date,
        _batch_id,
        _source_system,
        _file_name,
        _source_file_checksum,
        _record_hash

    FROM source

),

deduped AS (

    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY event_id
               ORDER BY _ingestion_ts DESC
           ) AS rn
    FROM parsed

)

SELECT
    event_id,
    application_id,
    old_status,
    new_status,
    event_timestamp,
    event_date,
    _ingestion_ts,
    _ingestion_date,
    _batch_id,
    _source_system,
    _file_name,
    _source_file_checksum,
    _record_hash,
    CURRENT_TIMESTAMP AS _processed_ts
FROM deduped
WHERE rn = 1
