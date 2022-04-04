
{% macro retrieve_edfi_records_from_data_lake(table_name) %}

WITH latest_extract AS (

    SELECT
        school_year,
        MAX(date_extracted) AS date_extracted
    FROM {{ source('staging', table_name) }}
    WHERE is_complete_extract IS TRUE
    GROUP BY 1

),

records AS (

    SELECT base_table.*
    FROM {{ source('staging', table_name) }} base_table
    LEFT JOIN latest_extract ON base_table.school_year = latest_extract.school_year
    WHERE
        id IS NOT NULL
        AND (
            latest_extract.date_extracted IS NULL
            OR base_table.date_extracted >= latest_extract.date_extracted)

)

{% endmacro %}
