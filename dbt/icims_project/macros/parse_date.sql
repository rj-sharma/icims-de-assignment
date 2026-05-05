{% macro parse_date(column_name) %}
    COALESCE(
        TRY_CAST(TRIM({{ column_name }}) AS DATE),

        -- ISO formats
        TRY_STRPTIME(TRIM({{ column_name }}), '%Y-%m-%d'),
        TRY_STRPTIME(TRIM({{ column_name }}), '%Y/%m/%d'),
        TRY_STRPTIME(TRIM({{ column_name }}), '%Y.%m.%d'),

        -- numeric formats
        TRY_STRPTIME(TRIM({{ column_name }}), '%d-%m-%Y'),
        TRY_STRPTIME(TRIM({{ column_name }}), '%m/%d/%Y'),
        TRY_STRPTIME(TRIM({{ column_name }}), '%m.%d.%Y'),

        -- textual month formats
        TRY_STRPTIME(TRIM({{ column_name }}), '%B %d, %Y'),  -- December 22, 2024
        TRY_STRPTIME(TRIM({{ column_name }}), '%b %d, %Y'),  -- Dec 22, 2024

        -- hyphenated textual formats
        TRY_STRPTIME(TRIM({{ column_name }}), '%d-%b-%Y'),   -- 30-Sep-2025
        TRY_STRPTIME(TRIM({{ column_name }}), '%d-%B-%Y')    -- 30-September-2025
    )
{% endmacro %}