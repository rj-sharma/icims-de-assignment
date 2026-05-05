Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices


### Ingestion
For applications, the primary issue was inconsistent date formats.
I handled this using a reusable dbt macro to standardize parsing across models.

Even though the dataset didn’t show duplicates, I implemented batch-level deduplication using window functions to ensure robustness in production scenarios.

The staging model is incremental and batch-driven, allowing safe re-runs and backfills without duplication.

Data quality is enforced using dbt tests, especially ensuring that parsed dates are not null.


The jobs dataset required standardization of the posted_date, which I handled using a reusable dbt macro to ensure consistency across pipelines.

I also normalized categorical fields like department and status to enforce consistency and added validation using dbt tests to ensure data quality.

Since workflow events are event streams without a natural primary key, I generated a surrogate key using a hash of application_id, timestamp, and status.

I also implemented deduplication logic based on business semantics rather than raw row uniqueness.

Additionally, I added anomaly detection for cases where a “Hired” event occurs before the application date, which indicates data quality issues.


🔹 Data Quality & Validation Framework

Data quality checks are implemented using dbt tests, ensuring both schema-level and business-level validation across datasets.

Custom Business Rule Validation

A key business rule implemented:

A candidate cannot be marked as HIRED before applying.

This is validated using a custom dbt test:

SELECT
    w.application_id,
    w.event_timestamp,
    a.apply_date
FROM {{ ref('stg_workflow_events') }} w
JOIN {{ ref('stg_applications') }} a
    ON w.application_id = a.application_id
WHERE w.new_status = 'HIRED'
AND w.event_timestamp < a.apply_date

dbt test --select hired_before_applied --store-failures   

run on duck db - SELECT * 
FROM icims.main_dbt_test__audit.hired_before_applied;

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