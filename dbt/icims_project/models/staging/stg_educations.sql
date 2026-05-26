{{ config(
    materialized='incremental',
    unique_key='candidate_id',
    tags=['stg']
) }}

WITH source AS (

    SELECT *
    FROM {{ source('raw', 'education') }}
    WHERE _ingestion_date = CAST('{{ var("run_date") }}' AS DATE)

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

        -- normalize degree 
        UPPER(TRIM(degree)) AS degree,

        TRIM(institution) AS institution,
        year,

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
FROM standardized
