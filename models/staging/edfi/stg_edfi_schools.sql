
{{ retrieve_edfi_records_from_data_lake('base_edfi_schools') }}

SELECT
    date_extracted                          AS date_extracted,
    school_year                             AS school_year,
    id                                      AS id,
    JSON_VALUE(data, '$.localEducationAgencyReference.localEducationAgencyId') AS local_education_agency_id,
    JSON_VALUE(data, '$.schoolId')          AS school_id,
    JSON_VALUE(data, '$.nameOfInstitution') AS name_of_institution,
    SPLIT(JSON_VALUE(data, '$.schoolTypeDescriptor'), '#')[OFFSET(1)] AS school_type_descriptor,
FROM records

{{ remove_edfi_deletes_and_duplicates() }}
