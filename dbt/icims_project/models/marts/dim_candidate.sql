{{ config(
    materialized='incremental',
    unique_key='candidate_id',
    tags=['dim']
) }}

WITH base AS (

    SELECT *
    FROM {{ ref('stg_candidates') }}

    {% if is_incremental() %}
    WHERE DATE(_ingestion_ts) = '{{ var("run_date") }}'
    {% endif %}

),

education AS (

    SELECT
        candidate_id,
        degree,
        institution,
        year
    FROM {{ ref('stg_educations') }}

),

joined AS (

    SELECT
        b.candidate_id,
        b.first_name,
        b.last_name,
        b.email,
        b.phone,
        b.skills,
        b._ingestion_ts,

        e.degree,
        e.institution,
        e.year

    FROM base b
    LEFT JOIN education e
        ON b.candidate_id = e.candidate_id
),

deduped AS (

    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY candidate_id
               ORDER BY _ingestion_ts DESC
           ) AS rn
    FROM joined

)

SELECT
    candidate_id,
    first_name,
    last_name,
    email,
    phone,
    skills,
    degree,
    institution,
    year,

    CURRENT_TIMESTAMP AS _updated_at

FROM deduped
WHERE rn = 1