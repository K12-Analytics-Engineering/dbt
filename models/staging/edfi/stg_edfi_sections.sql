
WITH records AS (

    SELECT *
    FROM {{ source('staging', 'base_edfi_sections') }}
    WHERE date_extracted >= (
        SELECT MAX(date_extracted) AS date_extracted
        FROM {{ source('staging', 'base_edfi_sections') }}
        WHERE is_complete_extract IS TRUE)

)


SELECT
    date_extracted                          AS date_extracted,
    school_year                             AS school_year,
    id                                      AS id,
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
FROM records
WHERE
    extract_type = 'records'
    AND id NOT IN (SELECT id FROM records WHERE extract_type = 'deletes') 
QUALIFY ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY date_extracted DESC) = 1
