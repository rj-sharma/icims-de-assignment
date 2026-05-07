-- depends_on: {{ ref('stg_workflow_events') }}

{{ config(
    materialized='incremental',
    unique_key='application_id',
    tags=['fact']
) }}

WITH base_applications AS (

    SELECT *
    FROM {{ ref('stg_applications') }}

),

-- identify impacted applications
changed_applications AS (

    {% if is_incremental() %}

    SELECT DISTINCT application_id
    FROM {{ ref('stg_workflow_events') }}
    WHERE _ingestion_date = CAST('{{ var("run_date") }}' AS DATE)

    UNION

    SELECT DISTINCT application_id
    FROM {{ ref('stg_applications') }}
    WHERE _ingestion_date = CAST('{{ var("run_date") }}' AS DATE)

    {% else %}

    SELECT application_id FROM base_applications

    {% endif %}

),

filtered_apps AS (

    SELECT a.*
    FROM base_applications a
    JOIN changed_applications c
        ON a.application_id = c.application_id

),

enriched_events AS (

    SELECT e.*
    FROM {{ ref('int_workflow_events_enriched') }} e
    JOIN changed_applications c
        ON e.application_id = c.application_id

),

hired_events AS (

    SELECT
        application_id,
        MIN(event_timestamp) AS hired_date
    FROM enriched_events
    WHERE new_status = 'HIRED'
      AND is_anomaly = FALSE
    GROUP BY application_id

),

latest_status AS (

    SELECT
        application_id,
        new_status AS current_status
    FROM (

        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY application_id
                   ORDER BY event_timestamp DESC, event_sequence DESC
               ) AS rn
        FROM enriched_events
    )
    WHERE rn = 1

)

SELECT
    a.application_id,
    a.job_id,
    a.candidate_id,
    a.apply_date,

    h.hired_date,
    l.current_status,

    CASE 
        WHEN h.hired_date IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS is_hired,

    CASE 
        WHEN h.hired_date IS NOT NULL 
        THEN DATE_DIFF('day', a.apply_date, h.hired_date)
        ELSE NULL
    END AS time_to_hire_days,

    a._ingestion_ts,
    a._ingestion_date,
    a._batch_id,

    CURRENT_TIMESTAMP AS _updated_at

FROM filtered_apps a
LEFT JOIN hired_events h
    ON a.application_id = h.application_id
LEFT JOIN latest_status l
    ON a.application_id = l.application_id
