{% macro parse_date(column_name) %}
    COALESCE(
        TRY_CAST({{ column_name }} AS DATE),

        -- ISO formats
        TRY_STRPTIME({{ column_name }}, '%Y-%m-%d'),
        TRY_STRPTIME({{ column_name }}, '%Y/%m/%d'),
        TRY_STRPTIME({{ column_name }}, '%Y.%m.%d'), 

        -- numeric formats
        TRY_STRPTIME({{ column_name }}, '%d-%m-%Y'),
        TRY_STRPTIME({{ column_name }}, '%m/%d/%Y'),

        -- textual month formats (NEW)
        TRY_STRPTIME({{ column_name }}, '%B %d, %Y'),  -- December 22, 2024
        TRY_STRPTIME({{ column_name }}, '%b %d, %Y')   -- Dec 22, 2024
    )
{% endmacro %}