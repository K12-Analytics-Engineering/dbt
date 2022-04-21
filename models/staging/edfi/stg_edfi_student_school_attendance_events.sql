
{{ retrieve_edfi_records_from_data_lake('base_edfi_student_school_attendance_events') }}

select
    date_extracted                          as date_extracted,
    school_year                             as school_year,
    id                                      as id,
    struct(
        json_value(data, '$.studentReference.studentUniqueId') as student_unique_id
    ) as student_reference,
    parse_date('%Y-%m-%d', json_value(data, '$.eventDate')) as event_date,
    struct(
        json_value(data, '$.schoolReference.schoolId') as school_id
    ) as school_reference,
    struct(
        json_value(data, '$.sessionReference.schoolId') as school_id,
        cast(json_value(data, '$.sessionReference.schoolYear') as int64) as school_year,
        json_value(data, '$.sessionReference.sessionName') as session_name
    ) as session_reference,
    json_value(data, '$.arrivalTime') as arrival_time,
    json_value(data, '$.attendanceEventReason') as attendance_event_reason,
    json_value(data, '$.departureTime') as departure_time,
    cast(json_value(data, '$.eventDuration') as float64) as event_duration,
    cast(json_value(data, '$.schoolAttendanceDuration') as float64) as school_attendance_duration,
    split(json_value(data, '$.attendanceEventCategoryDescriptor'), '#')[OFFSET(1)] as attendance_event_category_descriptor,
    split(json_value(data, '$.educationalEnvironmentDescriptor'), '#')[OFFSET(1)] as educational_environment_descriptor,
from records

{{ remove_edfi_deletes_and_duplicates() }}
