-- Task 1: Basic SQL Analysis
--
-- These queries assume the local pipeline has already been run:
--
--   bash scripts/run_pipeline.sh --date 2026-05-07 --full
--
-- The queries use dbt staging models because staging is where source data has
-- been cleaned, typed, normalized, and deduplicated.

-- 1. How many jobs are currently open?
SELECT
    COUNT(*) AS open_jobs
FROM main_staging.stg_jobs
WHERE status = 'OPEN';

-- Expected result:
-- open_jobs
-- 178


-- 2. List candidates who applied to more than 3 distinct jobs.
SELECT
    COUNT(*) AS candidates_with_more_than_3_jobs
FROM (
    SELECT
        candidate_id,
        COUNT(DISTINCT job_id) AS jobs_applied
    FROM main_staging.stg_applications
    GROUP BY candidate_id
    HAVING COUNT(DISTINCT job_id) > 3
) candidates;

-- Expected result:
-- candidates_with_more_than_3_jobs
-- 506


-- 3. Top 5 departments by number of applications.
SELECT
    j.department,
    COUNT(a.application_id) AS total_applications
FROM main_staging.stg_applications a
JOIN main_staging.stg_jobs j
    ON a.job_id = j.job_id
GROUP BY j.department
ORDER BY total_applications DESC
LIMIT 5;

-- Expected result:
-- department    total_applications
-- MARKETING     923
-- PRODUCT       810
-- ENGINEERING   789
-- SALES         761
-- FINANCE       629
