
{{ retrieve_edfi_records_from_data_lake('base_edfi_student_school_attendance_events') }}

SELECT
    date_extracted                          AS date_extracted,
    school_year                             AS school_year,
    id                                      AS id,
    STRUCT(
        JSON_VALUE(data, '$.studentReference.studentUniqueId') AS student_unique_id
    ) AS student_reference,
    PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, '$.eventDate')) AS event_date,
    STRUCT(
        JSON_VALUE(data, '$.schoolReference.schoolId') AS school_id
    ) AS school_reference,
    STRUCT(
        JSON_VALUE(data, '$.sessionReference.schoolId') AS school_id,
        CAST(JSON_VALUE(data, '$.sessionReference.schoolYear') AS int64) AS school_year,
        JSON_VALUE(data, '$.sessionReference.sessionName') AS session_name
    ) AS session_reference,
    JSON_VALUE(data, '$.arrivalTime') AS arrival_time,
    JSON_VALUE(data, '$.attendanceEventReason') AS attendance_event_reason,
    JSON_VALUE(data, '$.departureTime') AS departure_time,
    CAST(JSON_VALUE(data, '$.eventDuration') AS float64) AS event_duration,
    CAST(JSON_VALUE(data, '$.schoolAttendanceDuration') AS float64) AS school_attendance_duration,
    SPLIT(JSON_VALUE(data, '$.attendanceEventCategoryDescriptor'), '#')[OFFSET(1)] AS attendance_event_category_descriptor,
    SPLIT(JSON_VALUE(data, '$.educationalEnvironmentDescriptor'), '#')[OFFSET(1)] AS educational_environment_descriptor,
FROM records

{{ remove_edfi_deletes_and_duplicates() }}
