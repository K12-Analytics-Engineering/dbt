
{{ retrieve_edfi_records_from_data_lake('base_edfi_staff_school_associations') }}

select
    date_extracted                          as date_extracted,
    school_year                             as school_year,
    id                                      as id,
    struct(
        json_value(data, '$.staffReference.staffUniqueId') as staff_unique_id
    ) as staff_reference,
    struct(
        json_value(data, '$.schoolReference.schoolId') as school_id
    ) as school_reference,
    struct(
        cast(json_value(data, '$.schoolYearTypeReference.schoolYear') as int64) as school_year
    ) as school_year_type_reference,
    struct(
        json_value(data, '$.calendarReference.calendarCode') as calendar_code,
        json_value(data, '$.calendarReference.schoolId') as school_id,
        cast(json_value(data, '$.calendarReference.schoolYear') as int64) as school_year
    ) as calendar_reference,
    split(json_value(data, "$.programAssignmentDescriptor"), '#')[OFFSET(1)] as program_assignment_descriptor,
    array(
        select as struct 
            split(json_value(academic_subjects, "$.academicSubjectDescriptor"), '#')[OFFSET(1)] as academic_subject_descriptor,
        from unnest(json_query_array(data, "$.academicSubjects")) academic_subjects 
    ) as academic_subjects,
    array(
        select as struct 
            split(json_value(grade_levels, "$.gradeLevelDescriptor"), '#')[OFFSET(1)] as grade_level_descriptor,
        from unnest(json_query_array(data, "$.gradeLevels")) grade_levels 
    ) as grade_levels,
from records

{{ remove_edfi_deletes_and_duplicates() }}
