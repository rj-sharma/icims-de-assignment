{{ config(
    materialized='incremental',
    unique_key='job_id',
    tags=['dim']
) }}

WITH source AS (

    SELECT *
    FROM {{ ref('stg_jobs') }}

    {% if is_incremental() %}
    WHERE _ingestion_date = CAST('{{ var("run_date") }}' AS DATE)
    {% endif %}

),

deduped AS (

    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY job_id
               ORDER BY _ingestion_ts DESC
           ) AS rn
    FROM source

)

SELECT
    job_id,
    title,
    department,
    posted_date,
    status,

    CURRENT_TIMESTAMP AS _updated_at

FROM deduped
WHERE rn = 1
