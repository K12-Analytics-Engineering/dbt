
{{ retrieve_edfi_records_from_data_lake('base_edfi_students') }}

SELECT
    date_extracted                          AS date_extracted,
    school_year                             AS school_year,
    id                                      AS id,
    JSON_VALUE(data, '$.studentUniqueId') AS student_unique_id,
    JSON_VALUE(data, '$.lastSurname') AS last_surname,
    JSON_VALUE(data, '$.middleName') AS middle_name,
    JSON_VALUE(data, '$.firstName') AS first_name,
    JSON_VALUE(data, '$.generationCodeSuffix') AS generation_code_suffix,
    PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, '$.birthDate')) AS birth_date,
    JSON_VALUE(data, '$.birthCity') AS birth_city,
    SPLIT(JSON_VALUE(data, "$.birthCountryDescriptor"), '#')[OFFSET(1)] AS birth_country_descriptor,
    JSON_VALUE(data, '$.birthInternationalProvince') AS birth_international_province,
    STRUCT(
        JSON_VALUE(data, '$.personReference.personId') AS person_id,
        SPLIT(JSON_VALUE(data, "$.personReference.sourceSystemDescriptor"), '#')[OFFSET(1)] AS source_system_descriptor
    ) AS person_reference,
FROM records

{{ remove_edfi_deletes_and_duplicates() }}
