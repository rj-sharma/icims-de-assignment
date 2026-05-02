SELECT
    TRIM(application_id) AS application_id,
    TRIM(old_status) AS old_status,
    TRIM(new_status) AS new_status,
    CAST(event_timestamp AS TIMESTAMP) AS event_timestamp
FROM {{ source('raw', 'workflow_events') }}
WHERE application_id IS NOT NULL