
{{ retrieve_edfi_records_from_data_lake('base_edfi_staff_section_associations') }}

select
    date_extracted                          as date_extracted,
    school_year                             as school_year,
    id                                      as id,
    struct(
        json_value(data, '$.staffReference.staffUniqueId') as staff_unique_id
    ) as staff_reference,
    struct(
        json_value(data, '$.sectionReference.localCourseCode') as local_course_code,
        json_value(data, '$.sectionReference.schoolId') as school_id,
        cast(json_value(data, '$.sectionReference.schoolYear') as int64) as school_year,
        json_value(data, '$.sectionReference.sectionIdentifier') as section_identifier,
        json_value(data, '$.sectionReference.sessionName') as session_name
    ) as section_reference,
    parse_date('%Y-%m-%d', json_value(data, "$.beginDate")) as begin_date,
    parse_date('%Y-%m-%d', json_value(data, "$.endDate")) as end_date,
    split(json_value(data, "$.classroomPositionDescriptor"), '#')[OFFSET(1)] as classroom_position_descriptor,
    cast(json_value(data, '$.highlyQualifiedTeacher') as BOOL) as highly_qualified_teacher,
    json_value(data, '$.percentageContribution') as percentage_contribution
from records

{{ remove_edfi_deletes_and_duplicates() }}
