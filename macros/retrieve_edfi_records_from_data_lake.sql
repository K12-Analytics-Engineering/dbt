
{% macro retrieve_edfi_records_from_data_lake(table_name) %}

with latest_extract as (

    select
        school_year,
        max(date_extracted) as date_extracted
    from {{ source('staging', table_name) }}
    where is_complete_extract is true
    group by 1

),

records as (

    select base_table.*
    from {{ source('staging', table_name) }} base_table
    left join latest_extract on base_table.school_year = latest_extract.school_year
    where
        id is not null
        and (
            latest_extract.date_extracted is null
            or base_table.date_extracted >= latest_extract.date_extracted)

)

{% endmacro %}
