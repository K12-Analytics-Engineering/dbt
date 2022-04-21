
{% macro remove_edfi_deletes_and_duplicates() %}

where
    extract_type = 'records'
    and id not in (select id from records where extract_type = 'deletes') 
qualify row_number() over (
        partition by id
        order by date_extracted DESC) = 1

{% endmacro %}
