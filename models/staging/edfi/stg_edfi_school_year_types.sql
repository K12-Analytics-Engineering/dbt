
{{ retrieve_edfi_records_from_data_lake('base_edfi_school_year_types') }}

select distinct
    date_extracted                                  as date_extracted,
    cast(json_value(data, '$.schoolYear') as int64) as school_year,
    id                                      as id,
    json_value(data, '$.schoolYearDescription')     as school_year_description
from records

{{ remove_edfi_deletes_and_duplicates() }}
