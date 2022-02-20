
WITH parsed_data AS (

    SELECT
        JSON_VALUE(data, '$.extractedTimestamp') AS extracted_timestamp,
        JSON_VALUE(data, '$.id') AS id,
        CAST(JSON_VALUE(data, '$.schoolYear') AS int64) school_year,
        STRUCT(
            JSON_VALUE(data, '$.staffReference.staffUniqueId') AS staff_unique_id
        ) AS staff_reference,
        STRUCT(
            JSON_VALUE(data, '$.sectionReference.localCourseCode') AS local_course_code,
            JSON_VALUE(data, '$.sectionReference.schoolId') AS school_id,
            CAST(JSON_VALUE(data, '$.sectionReference.schoolYear') AS int64) AS school_year,
            JSON_VALUE(data, '$.sectionReference.sectionIdentifier') AS section_identifier,
            JSON_VALUE(data, '$.sectionReference.sessionName') AS session_name
        ) AS section_reference,
        PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, "$.beginDate")) AS begin_date,
        PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, "$.endDate")) AS end_date,
        SPLIT(JSON_VALUE(data, "$.classroomPositionDescriptor"), '#')[OFFSET(1)] AS classroom_position_descriptor,
        CAST(JSON_VALUE(data, '$.highlyQualifiedTeacher') AS BOOL) AS highly_qualified_teacher,
        JSON_VALUE(data, '$.percentageContribution') AS percentage_contribution
    FROM {{ source('staging', 'base_edfi_staff_section_associations') }}

),

ranked AS (

    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY
                school_year,
                section_reference.school_year,
                section_reference.school_id,
                section_reference.session_name,
                section_reference.local_course_code,
                section_reference.section_identifier,
                staff_reference.staff_unique_id
            ORDER BY school_year DESC, extracted_timestamp DESC
        ) AS rank,
        *
    FROM parsed_data

)

SELECT * EXCEPT (extracted_timestamp, rank, school_year),
    COALESCE(section_reference.school_year, school_year) AS school_year
FROM ranked
WHERE
    rank = 1
    AND id NOT IN (
        SELECT id FROM {{ ref('stg_edfi_deletes') }} edfi_deletes
        WHERE ranked.school_year = edfi_deletes.school_year
    )