
WITH parsed_data AS (

    SELECT
        CAST(JSON_VALUE(data, '$.extractedTimestamp') AS TIMESTAMP) AS extracted_timestamp,
        JSON_VALUE(data, '$.id') AS id,
        CAST(JSON_VALUE(data, '$.schoolYear') AS int64) school_year,
        JSON_VALUE(data, '$.localCourseCode') AS local_course_code,
        JSON_VALUE(data, '$.localCourseTitle') AS local_course_title,
        STRUCT(
            JSON_VALUE(data, '$.courseReference.courseCode') AS course_code,
            JSON_VALUE(data, '$.courseReference.educationOrganizationId') AS education_organization_id
        ) AS course_reference,
        STRUCT(
            JSON_VALUE(data, '$.schoolReference.schoolId') AS school_id
        ) AS school_reference,
        STRUCT(
            JSON_VALUE(data, '$.sessionReference.schoolId') AS school_id,
            CAST(JSON_VALUE(data, '$.sessionReference.schoolYear') AS int64) AS school_year,
            JSON_VALUE(data, '$.sessionReference.sessionName') AS session_name
        ) AS session_reference,
    FROM {{ source('staging', 'base_edfi_course_offerings') }}
    QUALIFY ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY extracted_timestamp DESC) = 1

)


SELECT * EXCEPT (school_year),
    COALESCE(session_reference.school_year, school_year) AS school_year
FROM parsed_data
WHERE
    id NOT IN (
        SELECT id FROM {{ ref('stg_edfi_deletes') }} edfi_deletes
        WHERE parsed_data.school_year = edfi_deletes.school_year)
