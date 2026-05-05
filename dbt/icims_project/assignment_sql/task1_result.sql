
How many jobs are currently open?
SELECT COUNT(*) AS open_jobs
FROM icims.main_staging.stg_jobs sj 
WHERE status = 'OPEN'; --178

List candidates who applied to more than 3 jobs
select count(*) from (
SELECT c.candidate_id,
      -- c.first_name ,
      -- c.last_name ,
       COUNT(DISTINCT a.job_id) AS jobs_applied
FROM icims.main_staging.stg_applications a
JOIN icims.main_staging.stg_candidates c 
    ON a.candidate_id = c.candidate_id
GROUP BY all
HAVING COUNT(DISTINCT a.job_id) > 3
) 506


Top 5 departments by number of applications
SELECT j.department,
       COUNT(a.application_id) AS total_applications
FROM icims.main_staging.stg_applications a
JOIN icims.main_staging.stg_jobs j 
    ON a.job_id = j.job_id
GROUP BY j.department
ORDER BY total_applications DESC
LIMIT 5;