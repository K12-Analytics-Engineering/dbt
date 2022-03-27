
WITH parsed_data AS (

    SELECT
        date_extracted                          AS date_extracted,
        school_year                             AS school_year,
        JSON_VALUE(data, '$.id') AS id,
        JSON_VALUE(data, '$.assessmentIdentifier') AS assessment_identifier,
        JSON_VALUE(data, '$.assessmentFamily') AS assessment_family,
        JSON_VALUE(data, '$.assessmentForm') AS assessment_form,
        JSON_VALUE(data, '$.assessmentTitle') AS assessment_title,
        CAST(JSON_VALUE(data, '$.assessmentVersion') AS int64) AS assessment_version,
        CAST(JSON_VALUE(data, '$.maxRawScore') AS float64) AS max_raw_score,
        JSON_VALUE(data, '$.namespace') AS namespace,
        JSON_VALUE(data, '$.nomenclature') AS nomenclature,
        CAST(JSON_VALUE(data, "$.adaptiveAssessment") AS BOOL) AS adaptive_assessment,
        SPLIT(JSON_VALUE(data, "$.assessmentCategoryDescriptor"), '#')[OFFSET(1)] AS assessment_category_descriptor,
        PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, "$.revisionDate")) AS revision_date,
        STRUCT(
            JSON_VALUE(data, '$.educationOrganizationReference.educationOrganizationId') AS education_organization_id
        ) AS education_organization_reference,
        ARRAY(
            SELECT AS STRUCT 
                SPLIT(JSON_VALUE(academic_subjects, "$.academicSubjectDescriptor"), '#')[OFFSET(1)] AS academic_subject_descriptor,
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.academicSubjects")) academic_subjects 
        ) AS academic_subjects,
        ARRAY(
            SELECT AS STRUCT 
                SPLIT(JSON_VALUE(grade_levels, "$.gradeLevelDescriptor"), '#')[OFFSET(1)] AS grade_level_descriptor
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.assessedGradeLevels")) grade_levels 
        ) AS assessed_grade_levels,
        ARRAY(
            SELECT AS STRUCT 
                SPLIT(JSON_VALUE(codes, "$.assessmentIdentificationSystemDescriptor"), '#')[OFFSET(1)] AS assessment_identification_system_descriptor,
                JSON_VALUE(codes, "$.assigningOrganizationIdentificationCode") AS assigning_organization_identification_code,
                JSON_VALUE(codes, "$.identificationCode") AS identification_code
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.identificationCodes")) codes 
        ) AS identification_codes,
        ARRAY(
            SELECT AS STRUCT 
                SPLIT(JSON_VALUE(languages, "$.languageDescriptor"), '#')[OFFSET(1)] AS language_descriptor,
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.languages")) languages 
        ) AS languages,
        ARRAY(
            SELECT AS STRUCT 
                SPLIT(JSON_VALUE(performance_levels, "$.assessmentReportingMethodDescriptor"), '#')[OFFSET(1)] AS assessment_reporting_method_descriptor,
                SPLIT(JSON_VALUE(performance_levels, "$.performanceLevelDescriptor"), '#')[OFFSET(1)] AS performance_level_descriptor,
                SPLIT(JSON_VALUE(performance_levels, "$.resultDatatypeTypeDescriptor"), '#')[OFFSET(1)] AS result_datatype_type_descriptor,
                JSON_VALUE(performance_levels, "$.maximumScore") AS maximum_score,
                JSON_VALUE(performance_levels, "$.minimumScore") AS minimum_score
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.performanceLevels")) performance_levels 
        ) AS performance_levels,
        ARRAY(
            SELECT AS STRUCT 
                SPLIT(JSON_VALUE(period, "$.assessmentPeriodDescriptor"), '#')[OFFSET(1)] AS assessment_period_descriptor,
                PARSE_DATE('%Y-%m-%d', JSON_VALUE(period, "$.beginDate")) AS begin_date,
                PARSE_DATE('%Y-%m-%d', JSON_VALUE(period, "$.endDate")) AS end_date
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.period")) period 
        ) AS period,
        ARRAY(
            SELECT AS STRUCT 
                SPLIT(JSON_VALUE(platform_types, "$.platformTypeDescriptor"), '#')[OFFSET(1)] AS platform_type_descriptor,
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.platformTypes")) platform_types 
        ) AS platform_types,
        ARRAY(
            SELECT AS STRUCT 
                SPLIT(JSON_VALUE(scores, "$.assessmentReportingMethodDescriptor"), '#')[OFFSET(1)] AS assessment_reporting_method_descriptor,
                SPLIT(JSON_VALUE(scores, "$.resultDatatypeTypeDescriptor"), '#')[OFFSET(1)] AS result_datatype_type_descriptor,
                JSON_VALUE(scores, "$.maximumScore") AS maximum_score,
                JSON_VALUE(scores, "$.minimumScore") AS minimum_score
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.scores")) scores 
        ) AS scores,
        ARRAY(
            SELECT AS STRUCT
                JSON_VALUE(sections, '$.sectionReference.localCourseCode') AS local_course_code,
                JSON_VALUE(sections, '$.sectionReference.schoolId') AS school_id,
                JSON_VALUE(sections, '$.sectionReference.schoolYear') AS school_year,
                JSON_VALUE(sections, '$.sectionReference.sectionIdentifier') AS section_identifier,
                JSON_VALUE(sections, '$.sectionReference.sessionName') AS session_name
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.sections")) sections 
        ) AS sections
    FROM {{ source('staging', 'base_edfi_assessments') }}
    WHERE date_extracted >= (
        SELECT MAX(date_extracted) AS date_extracted
        FROM {{ source('staging', 'base_edfi_assessments') }}
        WHERE is_complete_extract IS TRUE)
    QUALIFY ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY date_extracted DESC) = 1

)


SELECT *
FROM parsed_data
WHERE
    id NOT IN (
        SELECT id FROM {{ ref('stg_edfi_deletes') }} edfi_deletes
        WHERE parsed_data.school_year = edfi_deletes.school_year
    )
