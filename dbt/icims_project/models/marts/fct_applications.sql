WITH hired_events AS (
    SELECT
        application_id,
        MIN(event_timestamp) AS hired_date
    FROM {{ ref('stg_workflow_events') }}
    WHERE LOWER(new_status) = 'hired'
    GROUP BY application_id
),

latest_status AS (
    SELECT
        application_id,
        FIRST_VALUE(new_status) OVER (
            PARTITION BY application_id
            ORDER BY event_timestamp DESC
        ) AS current_status
    FROM {{ ref('stg_workflow_events') }}
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

    DATE_DIFF('day', a.apply_date, h.hired_date) AS time_to_hire

FROM {{ ref('stg_applications') }} a
LEFT JOIN hired_events h
    ON a.application_id = h.application_id
LEFT JOIN latest_status l
    ON a.application_id = l.application_id