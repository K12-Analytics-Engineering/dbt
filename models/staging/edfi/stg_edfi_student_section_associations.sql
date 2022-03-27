
WITH parsed_data AS (

    SELECT
        date_extracted                          AS date_extracted,
        school_year                             AS school_year,
        JSON_VALUE(data, '$.id') AS id,
        STRUCT(
            JSON_VALUE(data, '$.studentReference.studentUniqueId') AS student_unique_id
        ) AS student_reference,
        STRUCT(
            JSON_VALUE(data, '$.sectionReference.localCourseCode') AS local_course_code,
            JSON_VALUE(data, '$.sectionReference.schoolId') AS school_id,
            CAST(JSON_VALUE(data, '$.sectionReference.schoolYear') AS int64) AS school_year,
            JSON_VALUE(data, '$.sectionReference.sectionIdentifier') AS section_identifier,
            JSON_VALUE(data, '$.sectionReference.sessionName') AS session_name
        ) AS section_reference,
        PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, "$.beginDate")) AS begin_date,
        PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, "$.endDate")) AS end_date,
        SPLIT(JSON_VALUE(data, "$.attemptStatusDescriptor"), '#')[OFFSET(1)] AS attempt_status_descriptor,
        CAST(JSON_VALUE(data, '$.homeroomIndicator') AS BOOL) AS homeroom_indicator,
        SPLIT(JSON_VALUE(data, "$.repeatIdentifierDescriptor"), '#')[OFFSET(1)] AS repeat_identifier_descriptor,
        CAST(JSON_VALUE(data, '$.teacherStudentDataLinkExclusion') AS BOOL) AS teacher_student_data_link_exclusion
    FROM {{ source('staging', 'base_edfi_student_section_associations') }}
    WHERE date_extracted >= (
        SELECT MAX(date_extracted) AS date_extracted
        FROM {{ source('staging', 'base_edfi_student_section_associations') }}
        WHERE is_complete_extract IS TRUE)
    QUALIFY ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY date_extracted DESC) = 1

)


SELECT * EXCEPT (school_year),
    COALESCE(section_reference.school_year, school_year) AS school_year
FROM parsed_data
WHERE
    id NOT IN (
        SELECT id FROM {{ ref('stg_edfi_deletes') }} edfi_deletes
        WHERE parsed_data.school_year = edfi_deletes.school_year)
