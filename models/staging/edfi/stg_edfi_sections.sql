
WITH parsed_data AS (

    SELECT
        JSON_VALUE(data, '$.extractedTimestamp') AS extracted_timestamp,
        JSON_VALUE(data, '$.id') AS id,
        CAST(JSON_VALUE(data, '$.schoolYear') AS int64) school_year,
        JSON_VALUE(data, '$.sectionIdentifier') AS section_identifier,
        JSON_VALUE(data, '$.sectionName') AS section_name,
        STRUCT(
            JSON_VALUE(data, '$.courseOfferingReference.localCourseCode') AS local_course_code,
            JSON_VALUE(data, '$.courseOfferingReference.schoolId') AS school_id,
            CAST(JSON_VALUE(data, '$.courseOfferingReference.schoolYear') AS int64) AS school_year,
            JSON_VALUE(data, '$.courseOfferingReference.sessionName') AS session_name
        ) AS course_offering_reference,
        CAST(JSON_VALUE(data, '$.availableCreditConversion') AS float64) AS available_credit_conversion,
        CAST(JSON_VALUE(data, '$.availableCredits') AS float64) AS available_credits,
        SPLIT(JSON_VALUE(data, '$.availableCreditTypeDescriptor'), '#')[OFFSET(1)] AS available_credit_type_descriptor,
        SPLIT(JSON_VALUE(data, '$.educationalEnvironmentDescriptor'), '#')[OFFSET(1)] AS educational_environment_descriptor,
        STRUCT(
            JSON_VALUE(data, '$.locationReference.classroomIdentificationCode') AS classroom_identification_code,
            JSON_VALUE(data, '$.locationReference.schoolId') AS school_id
        ) AS location_reference,
        STRUCT(
            JSON_VALUE(data, '$.locationSchoolReference.schoolId') AS school_id
        ) AS location_school_reference,
        ARRAY(
            SELECT AS STRUCT 
                STRUCT(
                    JSON_VALUE(class_periods, "$.classPeriodReference.classPeriodName") AS class_period_name,
                    JSON_VALUE(class_periods, '$.classPeriodReference.schoolId') AS school_id
                ) AS class_period_reference
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.classPeriods")) class_periods 
        ) AS class_periods,
    FROM {{ source('staging', 'base_edfi_sections') }}

),

ranked AS (

    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY
                school_year,
                course_offering_reference.school_year,
                course_offering_reference.school_id,
                course_offering_reference.session_name,
                course_offering_reference.local_course_code,
                section_identifier
            ORDER BY school_year DESC, extracted_timestamp DESC
        ) AS rank,
        *
    FROM parsed_data

)

SELECT * EXCEPT (extracted_timestamp, rank, school_year),
    COALESCE(course_offering_reference.school_year, school_year) AS school_year
FROM ranked
WHERE
    rank = 1
    AND id NOT IN (
        SELECT id FROM {{ ref('stg_edfi_deletes') }} edfi_deletes
        WHERE ranked.school_year = edfi_deletes.school_year
    )
