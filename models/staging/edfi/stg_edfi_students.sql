
{{ retrieve_edfi_records_from_data_lake('base_edfi_students') }}

select
    date_extracted                          as date_extracted,
    school_year                             as school_year,
    id                                      as id,
    json_value(data, '$.studentUniqueId') as student_unique_id,
    json_value(data, '$.lastSurname') as last_surname,
    json_value(data, '$.middleName') as middle_name,
    json_value(data, '$.firstName') as first_name,
    json_value(data, '$.generationCodeSuffix') as generation_code_suffix,
    parse_date('%Y-%m-%d', json_value(data, '$.birthDate')) as birth_date,
    json_value(data, '$.birthCity') as birth_city,
    split(json_value(data, "$.birthCountryDescriptor"), '#')[OFFSET(1)] as birth_country_descriptor,
    json_value(data, '$.birthInternationalProvince') as birth_international_province,
    struct(
        json_value(data, '$.personReference.personId') as person_id,
        split(json_value(data, "$.personReference.sourceSystemDescriptor"), '#')[OFFSET(1)] as source_system_descriptor
    ) as person_reference,
from records

{{ remove_edfi_deletes_and_duplicates() }}
