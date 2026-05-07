{% test row_count_between(model, min_rows, max_rows) %}

SELECT
    COUNT(*) AS row_count
FROM {{ model }}
HAVING COUNT(*) < {{ min_rows }}
    OR COUNT(*) > {{ max_rows }}

{% endtest %}
