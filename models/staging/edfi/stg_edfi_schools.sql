
{{ retrieve_edfi_records_from_data_lake('base_edfi_schools') }}

select
    date_extracted                               as date_extracted,
    school_year                                  as school_year,
    id                                           as id,
    json_value(data, '$.localEducationAgencyReference.localEducationAgencyId') as lea_id,
    json_value(data, '$.schoolId')               as school_id,
    json_value(data, '$.nameOfInstitution')      as school_name,
    json_value(data, '$.shortNameOfInstitution') as school_short_name,
    split(json_value(data, '$.schoolTypeDescriptor'), '#')[offset(1)] as school_type,
    array(
        select as struct 
            split(json_value(grade_levels, "$.gradeLevelDescriptor"), '#')[offset(1)] as grade_level
        from unnest(json_query_array(data, "$.gradeLevels")) grade_levels 
    ) as grade_levels
from records

{{ remove_edfi_deletes_and_duplicates() }}
