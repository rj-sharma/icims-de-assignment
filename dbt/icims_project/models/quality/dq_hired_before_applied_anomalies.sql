{{ config(
    materialized='table',
    tags=['dq']
) }}

SELECT
    w.event_id,
    w.application_id,
    w.old_status,
    w.new_status,
    w.event_timestamp,
    a.apply_date,
    DATE_DIFF('day', w.event_timestamp, a.apply_date) AS days_before_apply,
    w._ingestion_date,
    w._batch_id,
    CURRENT_TIMESTAMP AS detected_at
FROM {{ ref('stg_workflow_events') }} w
JOIN {{ ref('stg_applications') }} a
    ON w.application_id = a.application_id
WHERE w.new_status = 'HIRED'
  AND w.event_timestamp < a.apply_date
