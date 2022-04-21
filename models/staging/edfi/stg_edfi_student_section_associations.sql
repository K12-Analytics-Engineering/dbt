
{{ retrieve_edfi_records_from_data_lake('base_edfi_student_section_associations') }}

select
    date_extracted                          as date_extracted,
    school_year                             as school_year,
    id                                      as id,
    struct(
        json_value(data, '$.studentReference.studentUniqueId') as student_unique_id
    ) as student_reference,
    struct(
        json_value(data, '$.sectionReference.localCourseCode') as local_course_code,
        json_value(data, '$.sectionReference.schoolId') as school_id,
        cast(json_value(data, '$.sectionReference.schoolYear') as int64) as school_year,
        json_value(data, '$.sectionReference.sectionIdentifier') as section_identifier,
        json_value(data, '$.sectionReference.sessionName') as session_name
    ) as section_reference,
    parse_date('%Y-%m-%d', json_value(data, "$.beginDate")) as begin_date,
    parse_date('%Y-%m-%d', json_value(data, "$.endDate")) as end_date,
    split(json_value(data, "$.attemptStatusDescriptor"), '#')[OFFSET(1)] as attempt_status_descriptor,
    cast(json_value(data, '$.homeroomIndicator') as BOOL) as homeroom_indicator,
    split(json_value(data, "$.repeatIdentifierDescriptor"), '#')[OFFSET(1)] as repeat_identifier_descriptor,
    cast(json_value(data, '$.teacherStudentDataLinkExclusion') as BOOL) as teacher_student_data_link_exclusion
from records

{{ remove_edfi_deletes_and_duplicates() }}
