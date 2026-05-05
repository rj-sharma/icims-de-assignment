{{ config(
    materialized='table'
) }}

WITH events AS (

    SELECT *
    FROM {{ ref('stg_workflow_events') }}

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

    a.apply_date,

    -- 🔥 anomaly flag
    CASE 
        WHEN e.new_status = 'HIRED'
             AND e.event_timestamp < a.apply_date
        THEN TRUE
        ELSE FALSE
    END AS is_anomaly,

    -- optional but strong
    ROW_NUMBER() OVER (
        PARTITION BY e.application_id
        ORDER BY e.event_timestamp
    ) AS event_sequence,

    e._ingestion_ts,
    e._batch_id,
    e._processed_ts

FROM events e
LEFT JOIN applications a
    ON e.application_id = a.application_id