
{{ retrieve_edfi_records_from_data_lake('base_edfi_local_education_agencies') }}

select
    date_extracted                          as date_extracted,
    school_year                             as school_year,
    id                                      as id,
    json_value(data, '$.localEducationAgencyId') as local_education_agency_id,
    json_value(data, '$.nameOfInstitution') as name_of_institution
from records

{{ remove_edfi_deletes_and_duplicates() }}
