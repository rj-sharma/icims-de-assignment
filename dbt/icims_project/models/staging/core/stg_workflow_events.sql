{{ config(
    materialized='incremental',
    unique_key='event_id'
) }}

WITH source AS (

    SELECT *
    FROM {{ source('raw', 'workflow_events') }}
    WHERE DATE(_ingestion_ts) = '{{ var("run_date") }}'

),

parsed AS (

    SELECT
        application_id,
        UPPER(TRIM(old_status)) AS old_status,
        UPPER(TRIM(new_status)) AS new_status,

        -- timestamp normalization
        TRY_CAST(event_timestamp AS TIMESTAMP) AS event_timestamp,

        _ingestion_ts,
        _batch_id

    FROM source

),

deduped AS (

    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY application_id, event_timestamp, new_status
               ORDER BY _ingestion_ts DESC
           ) AS rn
    FROM parsed

),

final AS (

    SELECT
        -- surrogate key (important)
        MD5(
            application_id || 
            CAST(event_timestamp AS VARCHAR) || 
            new_status
        ) AS event_id,

        application_id,
        old_status,
        new_status,
        event_timestamp,

        _ingestion_ts,
        _batch_id

    FROM deduped
    WHERE rn = 1

)

SELECT
    *,
    CURRENT_TIMESTAMP AS _processed_ts
FROM final