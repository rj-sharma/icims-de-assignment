{{ config(
    materialized='table',
    tags=['mart', 'metric']
) }}

SELECT
    j.job_id,
    j.title,
    j.department,
    COUNT(*) AS applications_count,
    SUM(CASE WHEN a.is_hired THEN 1 ELSE 0 END) AS hired_applications_count,
    AVG(a.time_to_hire_days) AS avg_time_to_hire_days,
    MIN(a.time_to_hire_days) AS min_time_to_hire_days,
    MAX(a.time_to_hire_days) AS max_time_to_hire_days,
    CURRENT_TIMESTAMP AS _updated_at
FROM {{ ref('fct_applications') }} a
LEFT JOIN {{ ref('dim_job') }} j
    ON a.job_id = j.job_id
WHERE a.is_hired
GROUP BY
    j.job_id,
    j.title,
    j.department
