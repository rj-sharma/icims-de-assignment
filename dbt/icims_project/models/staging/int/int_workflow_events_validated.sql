{{ config(materialized='table') }}

WITH base AS (
    SELECT
        NULLIF(TRIM(application_id), '') AS application_id,
        NULLIF(TRIM(old_status), '') AS old_status,
        NULLIF(TRIM(new_status), '') AS new_status,
        NULLIF(TRIM(event_timestamp), '') AS event_timestamp_raw
    FROM {{ source('raw', 'workflow_events') }}
)
SELECT
    application_id,
    old_status,
    new_status,
    event_timestamp_raw,
    COALESCE(
        TRY_CAST(event_timestamp_raw AS TIMESTAMP),
        TRY_STRPTIME(event_timestamp_raw, '%Y-%m-%d %H:%M:%S'),
        TRY_STRPTIME(event_timestamp_raw, '%Y/%m/%d %H:%M:%S'),
        TRY_STRPTIME(event_timestamp_raw, '%Y-%m-%dT%H:%M:%S')
    )::TIMESTAMP AS event_timestamp,
    REGEXP_MATCHES(
        LOWER(application_id),
        '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
    ) AS is_valid_application_id
FROM base
