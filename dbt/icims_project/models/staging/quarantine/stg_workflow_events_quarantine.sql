{{ config(materialized='table') }}

SELECT
    application_id,
    old_status,
    new_status,
    event_timestamp_raw,
    event_timestamp,
    CASE
        WHEN NOT is_valid_application_id THEN 'INVALID_APPLICATION_ID'
        WHEN new_status IS NULL THEN 'INVALID_NEW_STATUS'
        WHEN event_timestamp IS NULL THEN 'INVALID_EVENT_TIMESTAMP'
        ELSE 'UNKNOWN'
    END AS quarantine_reason
FROM {{ ref('int_workflow_events_validated') }}
WHERE NOT is_valid_application_id
   OR new_status IS NULL
   OR event_timestamp IS NULL
