
WITH parsed_data AS (

    SELECT
        CAST(JSON_VALUE(data, '$.extractedTimestamp') AS TIMESTAMP) AS extracted_timestamp,
        JSON_VALUE(data, '$.id') AS id,
        CAST(JSON_VALUE(data, '$.schoolYear') AS int64) school_year,
        CAST(JSON_VALUE(data, '$.numericGradeEarned') AS float64) AS numeric_grade_earned,
        JSON_VALUE(data, '$.letterGradeEarned') AS letter_grade_earned,
        SPLIT(JSON_VALUE(data, '$.performanceBaseConversionDescriptor'), '#')[OFFSET(1)] AS performance_base_conversion_descriptor, 
        SPLIT(JSON_VALUE(data, '$.gradeTypeDescriptor'), '#')[OFFSET(1)] AS grade_type_descriptor, 
        JSON_VALUE(data, '$.diagnosticStatement') AS diagnostic_statement,
        STRUCT(
            SPLIT(JSON_VALUE(data, '$.gradingPeriodReference.gradingPeriodDescriptor'), '#')[OFFSET(1)] AS grading_period_descriptor,
            CAST(JSON_VALUE(data, '$.gradingPeriodReference.periodSequence') AS int64) AS period_sequence,
            JSON_VALUE(data, '$.gradingPeriodReference.schoolId') AS school_id,
            CAST(JSON_VALUE(data, '$.gradingPeriodReference.schoolYear') AS int64) AS school_year
        ) AS grading_period_reference,
        STRUCT(
            EXTRACT(DATE FROM PARSE_TIMESTAMP('%Y-%m-%dT%TZ', JSON_VALUE(data, '$.studentSectionAssociationReference.beginDate'))) AS begin_date,
            JSON_VALUE(data, '$.studentSectionAssociationReference.localCourseCode') AS local_course_code,
            JSON_VALUE(data, '$.studentSectionAssociationReference.schoolId') AS school_id,
            CAST(JSON_VALUE(data, '$.studentSectionAssociationReference.schoolYear') AS int64) AS school_year,
            JSON_VALUE(data, '$.studentSectionAssociationReference.sectionIdentifier') AS section_identifier,
            JSON_VALUE(data, '$.studentSectionAssociationReference.sessionName') AS session_name,
            JSON_VALUE(data, '$.studentSectionAssociationReference.studentUniqueId') AS student_unique_id
        ) AS student_section_association_reference
    FROM {{ source('staging', 'base_edfi_grades') }}
    QUALIFY ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY extracted_timestamp DESC) = 1

)


SELECT * EXCEPT (school_year),
    COALESCE(grading_period_reference.school_year, school_year) AS school_year
FROM parsed_data
WHERE
    id NOT IN (
        SELECT id FROM {{ ref('stg_edfi_deletes') }} edfi_deletes
        WHERE parsed_data.school_year = edfi_deletes.school_year
    )
