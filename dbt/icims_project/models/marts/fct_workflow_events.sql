{{ config(
    materialized='incremental',
    unique_key='event_id',
    tags=['fact']
) }}

SELECT
    event_id,
    application_id,
    old_status,
    new_status,
    event_timestamp,
    CURRENT_TIMESTAMP AS _updated_at

FROM {{ ref('stg_workflow_events') }}

{% if is_incremental() %}
WHERE DATE(_ingestion_ts) = '{{ var("run_date") }}'
{% endif %}