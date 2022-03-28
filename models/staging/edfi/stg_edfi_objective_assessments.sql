
WITH records AS (

    SELECT *
    FROM {{ source('staging', 'base_edfi_objective_assessments') }}
    WHERE date_extracted >= (
        SELECT MAX(date_extracted) AS date_extracted
        FROM {{ source('staging', 'base_edfi_objective_assessments') }}
        WHERE is_complete_extract IS TRUE)

)


SELECT
    date_extracted                          AS date_extracted,
    school_year                             AS school_year,
    id                                      AS id,
    JSON_VALUE(data, '$.identificationCode') AS identification_code,
    SPLIT(JSON_VALUE(data, "$.academicSubjectDescriptor"), '#')[OFFSET(1)] AS academic_subject_descriptor,
    JSON_VALUE(data, '$.description') AS description,
    CAST(JSON_VALUE(data, '$.maxRawScore') AS float64) AS max_raw_score,
    CAST(JSON_VALUE(data, '$.percentOfAssessment') AS float64) AS percent_of_assessment,
    JSON_VALUE(data, '$.nomenclature') AS nomenclature,
    STRUCT(
        JSON_VALUE(data, '$.assessmentReference.assessmentIdentifier') AS assessment_identifier,
        JSON_VALUE(data, '$.assessmentReference.namespace') AS namespace
    ) AS assessment_reference,
    STRUCT(
        JSON_VALUE(data, '$.parentObjectiveAssessmentReference.assessmentIdentifier') AS assessment_identifier,
        JSON_VALUE(data, '$.parentObjectiveAssessmentReference.identificationCode') AS identification_code,
        JSON_VALUE(data, '$.parentObjectiveAssessmentReference.namespace') AS namespace
    ) AS parent_objective_assessment_reference,
    ARRAY(
        SELECT AS STRUCT 
            JSON_VALUE(assessment_items, '$.assessmentItemReference.assessmentIdentifier') AS assessment_identifier,
            JSON_VALUE(assessment_items, '$.assessmentItemReference.identificationCode') AS identification_code,
            JSON_VALUE(assessment_items, '$.assessmentItemReference.namespace') AS namespace
        FROM UNNEST(JSON_QUERY_ARRAY(data, "$.assessmentItems")) assessment_items 
    ) AS assessment_items,
    ARRAY(
        SELECT AS STRUCT
            STRUCT(
                JSON_VALUE(learning_objectives, '$.learningObjectiveReference.learningObjectiveId') AS learning_objective_id,
                JSON_VALUE(learning_objectives, '$.learningObjectiveReference.namespace') AS namespace
            ) AS learning_objective_reference
        FROM UNNEST(JSON_QUERY_ARRAY(data, "$.learningObjectives")) learning_objectives 
    ) AS learning_objectives,
    ARRAY(
        SELECT AS STRUCT
            STRUCT(
                JSON_VALUE(learning_standards, '$.learningStandardReference.learningStandardId') AS learning_standard_id
            ) AS learning_standard_reference 
        FROM UNNEST(JSON_QUERY_ARRAY(data, "$.learningStandards")) learning_standards
    ) AS learning_standards,
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
            SPLIT(JSON_VALUE(scores, "$.assessmentReportingMethodDescriptor"), '#')[OFFSET(1)] AS assessment_reporting_method_descriptor,
            SPLIT(JSON_VALUE(scores, "$.resultDatatypeTypeDescriptor"), '#')[OFFSET(1)] AS result_datatype_type_descriptor,
            JSON_VALUE(scores, "$.maximumScore") AS maximum_score,
            JSON_VALUE(scores, "$.minimumScore") AS minimum_score
        FROM UNNEST(JSON_QUERY_ARRAY(data, "$.scores")) scores 
    ) AS scores
FROM records
WHERE
    extract_type = 'records'
    AND id NOT IN (SELECT id FROM records WHERE extract_type = 'deletes') 
QUALIFY ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY date_extracted DESC) = 1
