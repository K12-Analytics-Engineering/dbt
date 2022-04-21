
{{ retrieve_edfi_records_from_data_lake('base_edfi_calendars') }}

select
    date_extracted                          as date_extracted,
    school_year                             as school_year,
    id                                      as id,
    json_value(data, '$.calendarCode') as calendar_code,
    struct(
        json_value(data, '$.schoolReference.schoolId') as school_id
    ) as school_reference,
    struct(
        cast(json_value(data, '$.schoolYearTypeReference.schoolYear') as int64) as school_year
    ) as school_year_type_reference,
    split(json_value(data, "$.calendarTypeDescriptor"), '#')[OFFSET(1)] as calendar_type_descriptor,
    array(
        select as struct 
            split(json_value(grade_levels, "$.gradeLevelDescriptor"), '#')[OFFSET(1)] as grade_level_descriptor
        from unnest(json_query_array(data, "$.gradeLevels")) grade_levels 
    ) as grade_levels
from records

{{ remove_edfi_deletes_and_duplicates() }}
