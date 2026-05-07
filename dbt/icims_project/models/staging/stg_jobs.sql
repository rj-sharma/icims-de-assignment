{{ config(
    materialized='incremental',
    unique_key='job_id',
    tags=['stg']
) }}

WITH source AS (

    SELECT *
    FROM {{ source('raw', 'jobs') }}
    WHERE _ingestion_date = CAST('{{ var("run_date") }}' AS DATE)

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
