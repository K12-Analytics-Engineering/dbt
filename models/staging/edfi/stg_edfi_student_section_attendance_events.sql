
{{ retrieve_edfi_records_from_data_lake('base_edfi_student_section_attendance_events') }}

select
    date_extracted                          as date_extracted,
    school_year                             as school_year,
    id                                      as id,
    struct(
        json_value(data, '$.studentReference.studentUniqueId') as student_unique_id
    ) as student_reference,
    parse_date('%Y-%m-%d', json_value(data, '$.eventDate')) as event_date,
    struct(
        json_value(data, '$.sectionReference.localCourseCode') as local_course_code,
        json_value(data, '$.sectionReference.schoolId') as school_id,
        cast(json_value(data, '$.sectionReference.schoolYear') as int64) as school_year,
        json_value(data, '$.sectionReference.sectionIdentifier') as section_identifier,
        json_value(data, '$.sectionReference.sessionName') as session_name
    ) as section_reference,
    array(
        select as struct 
            struct(
                json_value(class_periods, "$.classPeriodReference.classPeriodName") as class_period_name,
                json_value(class_periods, '$.classPeriodReference.schoolId') as school_id
            ) as class_period_reference
        from unnest(json_query_array(data, "$.classPeriods")) class_periods 
    ) as class_periods,
    json_value(data, '$.arrivalTime') as arrival_time,
    json_value(data, '$.departureTime') as departure_time,
    json_value(data, '$.attendanceEventReason') as attendance_event_reason,
    cast(json_value(data, '$.eventDuration') as float64) as event_duration,
    cast(json_value(data, '$.sectionAttendanceDuration') as float64) as section_attendance_duration,
    split(json_value(data, '$.attendanceEventCategoryDescriptor'), '#')[OFFSET(1)] as attendance_event_category_descriptor,
    split(json_value(data, '$.educationalEnvironmentDescriptor'), '#')[OFFSET(1)] as educational_environment_descriptor,
from records

{{ remove_edfi_deletes_and_duplicates() }}
