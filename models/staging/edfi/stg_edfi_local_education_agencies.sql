
{{ retrieve_edfi_records_from_data_lake('base_edfi_local_education_agencies') }}

SELECT
    date_extracted                          AS date_extracted,
    school_year                             AS school_year,
    id                                      AS id,
    JSON_VALUE(data, '$.localEducationAgencyId') AS local_education_agency_id,
    JSON_VALUE(data, '$.nameOfInstitution') AS name_of_institution
FROM records
WHERE
    extract_type = 'records'
    AND id NOT IN (SELECT id FROM records WHERE extract_type = 'deletes') 
QUALIFY ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY date_extracted DESC) = 1
