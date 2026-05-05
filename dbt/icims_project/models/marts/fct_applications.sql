{{ config(
    materialized='incremental',
    unique_key='application_id'
) }}

WITH base_applications AS (

    SELECT *
    FROM {{ ref('stg_applications') }}

),

-- identify impacted applications (correct incremental strategy)
changed_applications AS (

    {% if is_incremental() %}

    SELECT DISTINCT application_id
    FROM {{ ref('stg_workflow_events') }}
    WHERE DATE(_ingestion_ts) = '{{ var("run_date") }}'

    UNION

    SELECT DISTINCT application_id
    FROM {{ ref('stg_applications') }}
    WHERE DATE(_ingestion_ts) = '{{ var("run_date") }}'

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

-- 🔥 inline enrichment
enriched_events AS (

    SELECT
        w.application_id,
        w.new_status,
        w.event_timestamp,
        a.apply_date,

        CASE 
            WHEN w.new_status = 'HIRED'
                 AND w.event_timestamp < a.apply_date
            THEN TRUE
            ELSE FALSE
        END AS is_anomaly

    FROM {{ ref('stg_workflow_events') }} w
    LEFT JOIN base_applications a
        ON w.application_id = a.application_id

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
                   ORDER BY event_timestamp DESC
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

    CURRENT_TIMESTAMP AS _updated_at

FROM filtered_apps a
LEFT JOIN hired_events h
    ON a.application_id = h.application_id
LEFT JOIN latest_status l
    ON a.application_id = l.application_id