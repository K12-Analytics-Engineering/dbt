
WITH assessments AS (

    SELECT
        fct_student_assessment.school_year,
        fct_student_assessment.student_assessment_identifier,
        ARRAY_AGG(
            STRUCT(
                fct_student_assessment.reporting_method                    AS reporting_method,
                fct_student_assessment.student_result                      AS student_result
            )
        ) AS assessment_student_score
    FROM {{ ref('fct_student_assessment') }} fct_student_assessment
    LEFT JOIN {{ ref('dim_assessment') }} dim_assessment
        ON fct_student_assessment.assessment_key = dim_assessment.assessment_key
    WHERE dim_assessment.objective_assessment_identification_code IS NULL
    GROUP BY 1, 2

),

objective_assessments AS (

    SELECT
        fct_student_assessment.school_year,
        fct_student_assessment.student_assessment_identifier,
        ARRAY_AGG(
            STRUCT(
                dim_assessment.objective_assessment_identification_code              AS identification_code,
                dim_assessment.objective_assessment_description                      AS description,
                fct_student_assessment.reporting_method                                        AS reporting_method,
                fct_student_assessment.student_result                                          AS student_result
            )
        ) AS objective_assessment_student_score
    FROM {{ ref('fct_student_assessment') }} fct_student_assessment
    LEFT JOIN {{ ref('dim_assessment') }} dim_assessment
        ON fct_student_assessment.assessment_key = dim_assessment.assessment_key
    WHERE dim_assessment.objective_assessment_identification_code IS NOT NULL
    GROUP BY 1, 2

)

SELECT
    fct_student_assessment.school_year                          AS school_year,
    dim_school.school_name                                      AS school_name,
    dim_student.student_unique_id                               AS student_unique_id,
    dim_student.student_last_surname                            AS student_last_surname,
    dim_student.student_first_name                              AS student_first_name,
    dim_student.student_display_name                            AS student_display_name,
    dim_student.is_actively_enrolled_in_school                  AS is_actively_enrolled_in_school,
    dim_student.grade_level                                     AS grade_level,
    dim_student.grade_level_id                                  AS grade_level_id,
    dim_student.gender                                          AS gender,
    dim_student.limited_english_proficiency                     AS limited_english_proficiency,
    dim_student.is_english_language_learner                     AS is_english_language_learner,
    dim_student.in_special_education_program                    AS in_special_education_program,
    dim_student.is_hispanic                                     AS is_hispanic,
    dim_student.race_and_ethnicity_roll_up                      AS race_and_ethnicity_roll_up,
    dim_assessment.assessment_identifier                        AS assessment_identifier,
    dim_assessment.title                                        AS title,
    dim_assessment.namespace                                    AS namespace,
    fct_student_assessment.student_assessment_identifier        AS student_assessment_identifier,
    objective_assessments.objective_assessment_student_score    AS objective_assessment_student_score,
    assessments.assessment_student_score                        AS assessment_student_score
FROM {{ ref('fct_student_assessment') }} fct_student_assessment
LEFT JOIN {{ ref('dim_assessment') }} dim_assessment
    ON fct_student_assessment.assessment_key = dim_assessment.assessment_key
LEFT JOIN assessments
    ON fct_student_assessment.student_assessment_identifier = assessments.student_assessment_identifier
LEFT JOIN objective_assessments
    ON fct_student_assessment.student_assessment_identifier = objective_assessments.student_assessment_identifier
LEFT JOIN {{ ref('dim_student') }} dim_student
    ON fct_student_assessment.student_key = dim_student.student_key
LEFT JOIN {{ ref('dim_school') }} dim_school
    ON fct_student_assessment.school_key = dim_school.school_key
WHERE 
    dim_assessment.objective_assessment_identification_code IS NULL
    AND dim_student.student_unique_id IS NOT NULL
