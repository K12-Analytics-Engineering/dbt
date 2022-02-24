
WITH parsed_data AS (

    SELECT
        JSON_VALUE(data, '$.extractedTimestamp') AS extracted_timestamp,
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

),

ranked AS (

    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY
                school_year,
                grading_period_reference.school_year,
                grading_period_reference.grading_period_descriptor,
                grading_period_reference.period_sequence,
                student_section_association_reference.school_year,
                student_section_association_reference.school_id,
                student_section_association_reference.session_name,
                student_section_association_reference.local_course_code,
                student_section_association_reference.section_identifier,
                student_section_association_reference.student_unique_id,
                student_section_association_reference.begin_date,
                grade_type_descriptor
            ORDER BY school_year DESC, extracted_timestamp DESC
        ) AS rank,
        *
    FROM parsed_data

)

SELECT * EXCEPT (extracted_timestamp, rank, school_year),
    COALESCE(grading_period_reference.school_year, school_year) AS school_year
FROM ranked
WHERE
    rank = 1
    AND id NOT IN (
        SELECT id FROM {{ ref('stg_edfi_deletes') }} edfi_deletes
        WHERE ranked.school_year = edfi_deletes.school_year
    )
