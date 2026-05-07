{{ config(
    materialized='incremental',
    unique_key='candidate_id',
    tags=['dim']
) }}

WITH base AS (

    SELECT *
    FROM {{ ref('stg_candidates') }}

    {% if is_incremental() %}
    WHERE _ingestion_date = CAST('{{ var("run_date") }}' AS DATE)
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
        b.email_hash,
        b.phone,
        b.phone_hash,
        b.skills,
        b.skills_array,
        b.skills_normalized,
        b.skills_count,
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
    email_hash,
    phone,
    phone_hash,
    skills_array AS skills,
    json_object(
        'degree', degree,
        'institution', institution,
        'year', year
    ) AS education,

    CURRENT_TIMESTAMP AS _updated_at

FROM deduped
WHERE rn = 1
