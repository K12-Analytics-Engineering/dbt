
{{ retrieve_edfi_records_from_data_lake('base_edfi_local_education_agencies') }}

select
    date_extracted                                                           as date_extracted,
    school_year                                                              as school_year,
    id                                                                       as id,
    json_value(data, '$.localEducationAgencyId')                             as lea_id,
    json_value(data, '$.nameOfInstitution')                                  as lea_name,
    split(json_value(data, '$.localEducationAgencyCategoryDescriptor'), '#')[offset(1)] as lea_category,
    split(json_value(data, '$.operationalStatusDescriptor'), '#')[offset(1)]            as operational_status,
from records

{{ remove_edfi_deletes_and_duplicates() }}
