
WITH assessments AS (
    -- ie. Survey With Goals, Fall, Reading, Growth: Reading 6+ CCSS 2010 V4 (No TTS)
    SELECT DISTINCT
        test_type,
        term_name,
        course,
        test_name
    FROM {{ ref('stg_nwea_map_assessment_results') }}
    WHERE test_type = "Survey With Goals"

),

performance_levels AS (
    -- retrieve performance level metadata seen in assessment results file
    SELECT DISTINCT
        PerformanceLevels.AssessmentReportingMethodDescriptor,
        PerformanceLevels.PerformanceLevelDescriptor
    FROM {{ ref('nwea_map_edfi_student_assessments') }}
    CROSS JOIN UNNEST(PerformanceLevels) PerformanceLevels

),

performance_levels_array AS (

    SELECT
        ARRAY_AGG(
            STRUCT(
                AssessmentReportingMethodDescriptor,
                PerformanceLevelDescriptor
            )
        ) AS PerformanceLevels
    FROM performance_levels

),

score_results AS (
    -- retrieve score metadata seen in assessment results file
    SELECT DISTINCT
        ScoreResults.AssessmentReportingMethodDescriptor,
        ScoreResults.ResultDatatypeTypeDescriptor,
    FROM {{ ref('nwea_map_edfi_student_assessments') }}
    CROSS JOIN UNNEST(ScoreResults) ScoreResults

),

score_results_array AS (

    SELECT
        ARRAY_AGG(
            STRUCT(
                AssessmentReportingMethodDescriptor,
                ResultDatatypeTypeDescriptor
            )
        ) AS ScoreResults
    FROM score_results

)


SELECT
    CONCAT(
        test_type, "-",
        term_name, "-",
        course
    )                                                                   AS AssessmentIdentifier,
    "NWEA MAP Growth"                                                   AS AssessmentFamily,
    test_name                                                           AS AssessmentTitle,
    "uri://nwea.org"                                                    AS Namespace,
    TRUE                                                                AS AdaptiveAssessment,
    STRUCT(
        CASE term_name
            WHEN "Fall" THEN "uri://ed-fi.org/AssessmentPeriodDescriptor#BOY"
            WHEN "Winter" THEN "uri://ed-fi.org/AssessmentPeriodDescriptor#MOY"
            WHEN "Spring" THEN "uri://ed-fi.org/AssessmentPeriodDescriptor#EOY"
        END AS AssessmentPeriodDescriptor
    )                                                                   AS Period,
    ARRAY(
        SELECT AS STRUCT 
            CONCAT(
                "uri://ed-fi.org/AcademicSubjectDescriptor#",
                course
            ) AS AcademicSubjectDescriptor
    )                                                                   AS AcademicSubjects,
    performance_levels_array.PerformanceLevels                          AS PerformanceLevels,
    score_results_array.ScoreResults                                    AS Scores
FROM assessments
CROSS JOIN performance_levels_array
CROSS JOIN score_results_array
