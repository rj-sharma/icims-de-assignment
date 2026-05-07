{{ config(
    materialized='incremental',
    unique_key='event_id',
    tags=['int']
) }}

WITH impacted_applications AS (

    {% if is_incremental() %}

    SELECT DISTINCT application_id
    FROM {{ ref('stg_workflow_events') }}
    WHERE _ingestion_date = CAST('{{ var("run_date") }}' AS DATE)

    {% else %}

    SELECT DISTINCT application_id
    FROM {{ ref('stg_workflow_events') }}

    {% endif %}

),

events AS (

    SELECT e.*
    FROM {{ ref('stg_workflow_events') }} e
    JOIN impacted_applications i
        ON e.application_id = i.application_id

),

applications AS (

    SELECT
        application_id,
        apply_date
    FROM {{ ref('stg_applications') }}

)

SELECT
    e.event_id,
    e.application_id,
    e.old_status,
    e.new_status,
    e.event_timestamp,
    e.event_date,

    a.apply_date,

    -- anomaly flag
    CASE 
        WHEN e.new_status = 'HIRED'
             AND e.event_timestamp < a.apply_date
        THEN TRUE
        ELSE FALSE
    END AS is_anomaly,

    -- Recompute the full timeline for impacted applications so late events keep sequence correct.
    ROW_NUMBER() OVER (
        PARTITION BY e.application_id
        ORDER BY e.event_timestamp
    ) AS event_sequence,

    e._ingestion_ts,
    e._ingestion_date,
    e._batch_id,
    e._source_system,
    e._file_name,
    e._source_file_checksum,
    e._record_hash,
    e._processed_ts

FROM events e
LEFT JOIN applications a
    ON e.application_id = a.application_id
