SELECT *
FROM {{ ref('stg_workflow_events') }} w
JOIN {{ ref('stg_applications') }} a
    ON w.application_id = a.application_id
WHERE w.new_status = 'HIRED'
AND w.event_timestamp < a.apply_date