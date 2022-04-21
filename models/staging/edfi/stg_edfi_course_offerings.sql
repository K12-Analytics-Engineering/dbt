
{{ retrieve_edfi_records_from_data_lake('base_edfi_course_offerings') }}

select
    date_extracted                          as date_extracted,
    school_year                             as school_year,
    json_value(data, '$.id') as id,
    json_value(data, '$.localCourseCode') as local_course_code,
    json_value(data, '$.localCourseTitle') as local_course_title,
    struct(
        json_value(data, '$.courseReference.courseCode') as course_code,
        json_value(data, '$.courseReference.educationOrganizationId') as education_organization_id
    ) as course_reference,
    struct(
        json_value(data, '$.schoolReference.schoolId') as school_id
    ) as school_reference,
    struct(
        json_value(data, '$.sessionReference.schoolId') as school_id,
        cast(json_value(data, '$.sessionReference.schoolYear') as int64) as school_year,
        json_value(data, '$.sessionReference.sessionName') as session_name
    ) as session_reference,
from records

{{ remove_edfi_deletes_and_duplicates() }}
