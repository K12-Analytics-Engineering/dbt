
WITH goal_score_results AS (

        -- transform wide goal1, goal2, etc columns to long under goal columns
        SELECT
            test_id AS test_id,
            test_type AS test_type,
            term_name AS term_name,
            course AS academic_subject,
            goal_name,
            goal_adjective,
            goal_rit_score
        FROM {{ ref('stg_nwea_map_assessment_results') }}
        UNPIVOT(
            (goal_name, goal_adjective, goal_rit_score)
            FOR goals IN (
                (goal1_name, goal1_adjective, goal1_rit_score),
                (goal2_name, goal2_adjective, goal2_rit_score),
                (goal3_name, goal3_adjective, goal3_rit_score),
                (goal4_name, goal4_adjective, goal4_rit_score)
            )
        )

),

student_objective_assessments AS (

    -- transform goal data into objective assessment spec
    SELECT
        test_id,
        STRUCT(

                STRUCT(
                    CONCAT(
                        test_type, "-",
                        term_name, "-",
                        academic_subject
                    )                   AS AssessmentIdentifier,
                    goal_name AS IdentificationCode,
                    "uri://nwea.org" AS Namespace
                ) AS ObjectiveAssessmentReference
                ,
                ARRAY(
                    SELECT AS STRUCT
                        "uri://ed-fi.org/AssessmentReportingMethodDescriptor#Proficiency level"   AS AssessmentReportingMethodDescriptor,
                        CONCAT(
                            "uri://nwea.org/PerformanceLevelDescriptor#",
                            goal_adjective
                        )                                                                         AS PerformanceLevelDescriptor
                ) AS PerformanceLevels,
                ARRAY(
                    SELECT AS STRUCT
                        "uri://ed-fi.org/AssessmentReportingMethodDescriptor#RIT scale score"                AS AssessmentReportingMethodDescriptor,
                        "uri://ed-fi.org/ResultDatatypeTypeDescriptor#Integer"                              AS ResultDatatypeTypeDescriptor,
                        goal_rit_score                                                                      AS Result
                ) AS ScoreResults

        ) AS result
    FROM goal_score_results
    WHERE goal_name != ""

),

student_objective_assessments_array AS (

    -- aggregate object assessment data into an array
    SELECT
        test_id,
        ARRAY_AGG(result) AS student_objective_assessments
    FROM student_objective_assessments
    GROUP BY test_id

),

score_results AS (

    {% for score in [
        ["RIT scale score", "Integer", "test_rit_score"],
        ["Percentile", "Integer", "test_percentile"],
    ] %}

        SELECT
            test_id,
            STRUCT(
                "uri://ed-fi.org/AssessmentReportingMethodDescriptor#{{score[0]}}"      AS AssessmentReportingMethodDescriptor,
                "uri://ed-fi.org/ResultDatatypeTypeDescriptor#{{score[1]}}"             AS ResultDatatypeTypeDescriptor,
                {{score[2]}}                                                            AS Result
            ) AS result
        FROM {{ ref('stg_nwea_map_assessment_results') }} 
        WHERE test_type = "Survey With Goals" AND {{score[2]}} IS NOT NULL
            
            {% if not loop.last %} UNION ALL {% endif %}

        {% endfor %}

),

score_results_array AS (

    -- aggregate assessment score result structs into an array
    SELECT
        test_id,
        ARRAY_AGG(result) AS score_results
    FROM score_results
    GROUP BY test_id

),

performance_levels AS (

    {% for performance_level in [
        ["uri://nwea.org/AssessmentReportingMethodDescriptor#Fall-To-Winter Met Projected Growth", "met_fall_to_winter_projected_growth"],
    ] %}

        SELECT
            test_id,
            STRUCT(
                "{{performance_level[0]}}"                                      AS AssessmentReportingMethodDescriptor,
                CONCAT(
                    "uri://ed-fi.org/PerformanceLevelDescriptor#",
                    IF({{performance_level[1]}}, "Pass", "Fail")
                )                                                               AS PerformanceLevelDescriptor
            ) AS result
            FROM {{ ref('stg_nwea_map_assessment_results') }} 
            WHERE test_type = "Survey With Goals" AND {{performance_level[1]}} IS NOT NULL
            
            {% if not loop.last %} UNION ALL {% endif %}

        {% endfor %}

),

performance_levels_array AS (

    -- aggregate assessment performance levels structs into an array
    SELECT
        test_id,
        ARRAY_AGG(result) AS performance_levels
    FROM performance_levels
    GROUP BY test_id

)


SELECT
    stg_nwea_map_assessment_results.test_id                                AS StudentAssessmentIdentifier,
    STRUCT(
        CONCAT(
            test_type, "-",
            term_name, "-",
            course
        )                   AS AssessmentIdentifier,
        "uri://nwea.org"    AS Namespace
    )                                                                     AS AssessmentReference,
    STRUCT( school_year AS SchoolYear )                                   AS SchoolYearTypeReference,
    STRUCT( student_unique_id AS StudentUniqueId )                        AS StudentReference,
    test_start_date                                                       AS AdministrationDate,
    performance_levels_array.performance_levels                           AS PerformanceLevels,
    score_results_array.score_results                                     AS ScoreResults,
    student_objective_assessments_array.student_objective_assessments     AS StudentObjectiveAssessments
FROM {{ ref('stg_nwea_map_assessment_results') }} stg_nwea_map_assessment_results
LEFT JOIN student_objective_assessments_array ON stg_nwea_map_assessment_results.test_id = student_objective_assessments_array.test_id
LEFT JOIN score_results_array ON stg_nwea_map_assessment_results.test_id = score_results_array.test_id
LEFT JOIN performance_levels_array ON stg_nwea_map_assessment_results.test_id = performance_levels_array.test_id
WHERE stg_nwea_map_assessment_results.test_type = "Survey With Goals"
