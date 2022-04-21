
with goal_score_results as (

        -- transform wide goal1, goal2, etc columns to long under goal columns
        select
            test_id as test_id,
            test_type as test_type,
            term_name as term_name,
            course as academic_subject,
            goal_name,
            goal_adjective,
            goal_rit_score
        from {{ ref('stg_nwea_map_assessment_results') }}
        UNPIVOT(
            (goal_name, goal_adjective, goal_rit_score)
            FOR goals in (
                (goal1_name, goal1_adjective, goal1_rit_score),
                (goal2_name, goal2_adjective, goal2_rit_score),
                (goal3_name, goal3_adjective, goal3_rit_score),
                (goal4_name, goal4_adjective, goal4_rit_score)
            )
        )

),

student_objective_assessments as (

    -- transform goal data into objective assessment spec
    select
        test_id,
        struct(

                struct(
                    concat(
                        test_type, "-",
                        term_name, "-",
                        academic_subject
                    )                   as AssessmentIdentifier,
                    goal_name as IdentificationCode,
                    "uri://nwea.org" as Namespace
                ) as ObjectiveAssessmentReference
                ,
                array(
                    select as struct
                        "uri://ed-fi.org/AssessmentReportingMethodDescriptor#Proficiency level"   as AssessmentReportingMethodDescriptor,
                        concat(
                            "uri://nwea.org/PerformanceLevelDescriptor#",
                            goal_adjective
                        )                                                                         as PerformanceLevelDescriptor
                ) as PerformanceLevels,
                array(
                    select as struct
                        "uri://ed-fi.org/AssessmentReportingMethodDescriptor#RIT scale score"                as AssessmentReportingMethodDescriptor,
                        "uri://ed-fi.org/ResultDatatypeTypeDescriptor#Integer"                              as ResultDatatypeTypeDescriptor,
                        goal_rit_score                                                                      as Result
                ) as ScoreResults

        ) as result
    from goal_score_results
    where goal_name != ""

),

student_objective_assessments_array as (

    -- aggregate object assessment data into an array
    select
        test_id,
        ARRAY_AGG(result) as student_objective_assessments
    from student_objective_assessments
    group by test_id

),

score_results as (

    {% for score in [
        ["RIT scale score", "Integer", "test_rit_score"],
        ["Percentile", "Integer", "test_percentile"],
    ] %}

        select
            test_id,
            struct(
                "uri://ed-fi.org/AssessmentReportingMethodDescriptor#{{score[0]}}"      as AssessmentReportingMethodDescriptor,
                "uri://ed-fi.org/ResultDatatypeTypeDescriptor#{{score[1]}}"             as ResultDatatypeTypeDescriptor,
                {{score[2]}}                                                            as Result
            ) as result
        from {{ ref('stg_nwea_map_assessment_results') }} 
        where test_type = "Survey With Goals" and {{score[2]}} is not null
            
            {% if not loop.last %} union all {% endif %}

        {% endfor %}

),

score_results_array as (

    -- aggregate assessment score result structs into an array
    select
        test_id,
        ARRAY_AGG(result) as score_results
    from score_results
    group by test_id

),

performance_levels as (

    {% for performance_level in [
        ["uri://nwea.org/AssessmentReportingMethodDescriptor#Fall-To-Winter Met Projected Growth", "met_fall_to_winter_projected_growth"],
    ] %}

        select
            test_id,
            struct(
                "{{performance_level[0]}}"                                      as AssessmentReportingMethodDescriptor,
                concat(
                    "uri://ed-fi.org/PerformanceLevelDescriptor#",
                    if({{performance_level[1]}}, "Pass", "Fail")
                )                                                               as PerformanceLevelDescriptor
            ) as result
            from {{ ref('stg_nwea_map_assessment_results') }} 
            where test_type = "Survey With Goals" and {{performance_level[1]}} is not null
            
            {% if not loop.last %} union all {% endif %}

        {% endfor %}

),

performance_levels_array as (

    -- aggregate assessment performance levels structs into an array
    select
        test_id,
        ARRAY_AGG(result) as performance_levels
    from performance_levels
    group by test_id

)


select
    stg_nwea_map_assessment_results.test_id                                as StudentAssessmentIdentifier,
    struct(
        concat(
            test_type, "-",
            term_name, "-",
            course
        )                   as AssessmentIdentifier,
        "uri://nwea.org"    as Namespace
    )                                                                     as AssessmentReference,
    struct( school_year as SchoolYear )                                   as SchoolYearTypeReference,
    struct( student_unique_id as StudentUniqueId )                        as StudentReference,
    test_start_date                                                       as AdministrationDate,
    performance_levels_array.performance_levels                           as PerformanceLevels,
    score_results_array.score_results                                     as ScoreResults,
    student_objective_assessments_array.student_objective_assessments     as StudentObjectiveAssessments
from {{ ref('stg_nwea_map_assessment_results') }} stg_nwea_map_assessment_results
left join student_objective_assessments_array on stg_nwea_map_assessment_results.test_id = student_objective_assessments_array.test_id
left join score_results_array on stg_nwea_map_assessment_results.test_id = score_results_array.test_id
left join performance_levels_array on stg_nwea_map_assessment_results.test_id = performance_levels_array.test_id
where stg_nwea_map_assessment_results.test_type = "Survey With Goals"
