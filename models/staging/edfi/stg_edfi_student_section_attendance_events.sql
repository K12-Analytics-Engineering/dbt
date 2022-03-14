
WITH parsed_data AS (

    SELECT
        CAST(JSON_VALUE(data, '$.extractedTimestamp') AS TIMESTAMP) AS extracted_timestamp,
        JSON_VALUE(data, '$.id') AS id,
        CAST(JSON_VALUE(data, '$.schoolYear') AS int64) school_year,
        STRUCT(
            JSON_VALUE(data, '$.studentReference.studentUniqueId') AS student_unique_id
        ) AS student_reference,
        PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, '$.eventDate')) AS event_date,
        STRUCT(
            JSON_VALUE(data, '$.sectionReference.localCourseCode') AS local_course_code,
            JSON_VALUE(data, '$.sectionReference.schoolId') AS school_id,
            CAST(JSON_VALUE(data, '$.sectionReference.schoolYear') AS int64) AS school_year,
            JSON_VALUE(data, '$.sectionReference.sectionIdentifier') AS section_identifier,
            JSON_VALUE(data, '$.sectionReference.sessionName') AS session_name
        ) AS section_reference,
        ARRAY(
            SELECT AS STRUCT 
                STRUCT(
                    JSON_VALUE(class_periods, "$.classPeriodReference.classPeriodName") AS class_period_name,
                    JSON_VALUE(class_periods, '$.classPeriodReference.schoolId') AS school_id
                ) AS class_period_reference
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.classPeriods")) class_periods 
        ) AS class_periods,
        JSON_VALUE(data, '$.arrivalTime') AS arrival_time,
        JSON_VALUE(data, '$.departureTime') AS departure_time,
        JSON_VALUE(data, '$.attendanceEventReason') AS attendance_event_reason,
        CAST(JSON_VALUE(data, '$.eventDuration') AS float64) AS event_duration,
        CAST(JSON_VALUE(data, '$.sectionAttendanceDuration') AS float64) AS section_attendance_duration,
        SPLIT(JSON_VALUE(data, '$.attendanceEventCategoryDescriptor'), '#')[OFFSET(1)] AS attendance_event_category_descriptor,
        SPLIT(JSON_VALUE(data, '$.educationalEnvironmentDescriptor'), '#')[OFFSET(1)] AS educational_environment_descriptor,
    FROM {{ source('staging', 'base_edfi_student_section_attendance_events') }}
    QUALIFY ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY extracted_timestamp DESC) = 1

)


SELECT * EXCEPT (school_year),
    COALESCE(section_reference.school_year, school_year) AS school_year
FROM parsed_data
WHERE
    id NOT IN (
        SELECT id FROM {{ ref('stg_edfi_deletes') }} edfi_deletes
        WHERE parsed_data.school_year = edfi_deletes.school_year)
