{{ config(materialized='table') }}

SELECT
    application_id,
    old_status,
    new_status,
    event_timestamp
FROM {{ ref('int_workflow_events_validated') }}
WHERE is_valid_application_id
  AND new_status IS NOT NULL
  AND event_timestamp IS NOT NULL