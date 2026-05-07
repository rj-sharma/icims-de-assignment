-- depends_on: {{ ref('stg_workflow_events') }}

{{ config(
    materialized='incremental',
    unique_key='event_id',
    tags=['fact']
) }}

WITH impacted_applications AS (

    {% if is_incremental() %}

    SELECT DISTINCT application_id
    FROM {{ ref('stg_workflow_events') }}
    WHERE _ingestion_date = CAST('{{ var("run_date") }}' AS DATE)

    {% else %}

    SELECT DISTINCT application_id
    FROM {{ ref('int_workflow_events_enriched') }}

    {% endif %}

),

events AS (

    SELECT e.*
    FROM {{ ref('int_workflow_events_enriched') }} e
    JOIN impacted_applications i
        ON e.application_id = i.application_id

)

SELECT
    event_id,
    application_id,
    old_status,
    new_status,
    event_timestamp,
    event_date,
    event_sequence,
    is_anomaly,
    _ingestion_ts,
    _ingestion_date,
    _batch_id,
    CURRENT_TIMESTAMP AS _updated_at

FROM events
