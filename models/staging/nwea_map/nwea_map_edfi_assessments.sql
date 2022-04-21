
with assessments as (
    -- ie. Survey With Goals, Fall, Reading, Growth: Reading 6+ CCSS 2010 V4 (No TTS)
    select distinct
        test_type,
        term_name,
        course,
        test_name
    from {{ ref('stg_nwea_map_assessment_results') }}
    where test_type = "Survey With Goals"

),

performance_levels as (
    -- retrieve performance level metadata seen in assessment results file
    select distinct
        PerformanceLevels.AssessmentReportingMethodDescriptor,
        PerformanceLevels.PerformanceLevelDescriptor
    from {{ ref('nwea_map_edfi_student_assessments') }}
    cross join unnest(PerformanceLevels) PerformanceLevels

),

performance_levels_array as (

    select
        ARRAY_AGG(
            struct(
                AssessmentReportingMethodDescriptor,
                PerformanceLevelDescriptor
            )
        ) as PerformanceLevels
    from performance_levels

),

score_results as (
    -- retrieve score metadata seen in assessment results file
    select distinct
        ScoreResults.AssessmentReportingMethodDescriptor,
        ScoreResults.ResultDatatypeTypeDescriptor,
    from {{ ref('nwea_map_edfi_student_assessments') }}
    cross join unnest(ScoreResults) ScoreResults

),

score_results_array as (

    select
        ARRAY_AGG(
            struct(
                AssessmentReportingMethodDescriptor,
                ResultDatatypeTypeDescriptor
            )
        ) as ScoreResults
    from score_results

)


select
    concat(
        test_type, "-",
        term_name, "-",
        course
    )                                                                   as AssessmentIdentifier,
    "NWEA MAP Growth"                                                   as AssessmentFamily,
    test_name                                                           as AssessmentTitle,
    "uri://nwea.org"                                                    as Namespace,
    true                                                                as AdaptiveAssessment,
    struct(
        case term_name
            when "Fall" then "uri://ed-fi.org/AssessmentPeriodDescriptor#BOY"
            when "Winter" then "uri://ed-fi.org/AssessmentPeriodDescriptor#MOY"
            when "Spring" then "uri://ed-fi.org/AssessmentPeriodDescriptor#EOY"
        end as AssessmentPeriodDescriptor
    )                                                                   as Period,
    array(
        select as struct 
            concat(
                "uri://ed-fi.org/AcademicSubjectDescriptor#",
                course
            ) as AcademicSubjectDescriptor
    )                                                                   as AcademicSubjects,
    performance_levels_array.PerformanceLevels                          as PerformanceLevels,
    score_results_array.ScoreResults                                    as Scores
from assessments
cross join performance_levels_array
cross join score_results_array
