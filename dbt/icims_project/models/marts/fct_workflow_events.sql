SELECT
    ROW_NUMBER() OVER () AS event_id,
    application_id,
    old_status,
    new_status,
    event_timestamp
FROM {{ ref('stg_workflow_events') }}