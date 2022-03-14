
WITH parsed_data AS (

    SELECT
        JSON_VALUE(data, '$.extractedTimestamp') AS extracted_timestamp,
        JSON_VALUE(data, '$.id') AS id,
        CAST(JSON_VALUE(data, '$.schoolYear') AS int64) school_year,
        STRUCT(
            JSON_VALUE(data, '$.staffReference.staffUniqueId') AS staff_unique_id
        ) AS staff_reference,
        STRUCT(
            JSON_VALUE(data, '$.schoolReference.schoolId') AS school_id
        ) AS school_reference,
        STRUCT(
            CAST(JSON_VALUE(data, '$.schoolYearTypeReference.schoolYear') AS int64) AS school_year
        ) AS school_year_type_reference,
        STRUCT(
            JSON_VALUE(data, '$.calendarReference.calendarCode') AS calendar_code,
            JSON_VALUE(data, '$.calendarReference.schoolId') AS school_id,
            CAST(JSON_VALUE(data, '$.calendarReference.schoolYear') AS int64) AS school_year
        ) AS calendar_reference,
        SPLIT(JSON_VALUE(data, "$.programAssignmentDescriptor"), '#')[OFFSET(1)] AS program_assignment_descriptor,
        ARRAY(
            SELECT AS STRUCT 
                SPLIT(JSON_VALUE(academic_subjects, "$.academicSubjectDescriptor"), '#')[OFFSET(1)] AS academic_subject_descriptor,
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.academicSubjects")) academic_subjects 
        ) AS academic_subjects,
        ARRAY(
            SELECT AS STRUCT 
                SPLIT(JSON_VALUE(grade_levels, "$.gradeLevelDescriptor"), '#')[OFFSET(1)] AS grade_level_descriptor,
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.gradeLevels")) grade_levels 
        ) AS grade_levels,
    FROM {{ source('staging', 'base_edfi_staff_school_associations') }}
    QUALIFY ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY extracted_timestamp DESC) = 1

)


SELECT * EXCEPT (school_year),
    COALESCE(school_year_type_reference.school_year, school_year) AS school_year
FROM parsed_data
WHERE
    id NOT IN (
        SELECT id FROM {{ ref('stg_edfi_deletes') }} edfi_deletes
        WHERE parsed_data.school_year = edfi_deletes.school_year)
