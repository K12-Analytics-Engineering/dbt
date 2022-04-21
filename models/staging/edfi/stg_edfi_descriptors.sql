
{{ retrieve_edfi_records_from_data_lake('base_edfi_descriptors') }}

select
    date_extracted                          as date_extracted,
    school_year                             as school_year,
    id                                      as id,
    json_value(data, '$.codeValue') as code_value,
    json_value(data, '$.description') as description,
    json_value(data, '$.namespace') as namespace,
    json_value(data, '$.shortDescription') as short_description
from records

{{ remove_edfi_deletes_and_duplicates() }}
