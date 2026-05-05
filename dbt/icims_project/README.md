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
