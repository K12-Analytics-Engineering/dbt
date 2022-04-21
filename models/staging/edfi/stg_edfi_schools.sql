
{{ retrieve_edfi_records_from_data_lake('base_edfi_schools') }}

select
    date_extracted                          as date_extracted,
    school_year                             as school_year,
    id                                      as id,
    json_value(data, '$.localEducationAgencyReference.localEducationAgencyId') as local_education_agency_id,
    json_value(data, '$.schoolId')          as school_id,
    json_value(data, '$.nameOfInstitution') as name_of_institution,
    split(json_value(data, '$.schoolTypeDescriptor'), '#')[OFFSET(1)] as school_type_descriptor,
from records

{{ remove_edfi_deletes_and_duplicates() }}
