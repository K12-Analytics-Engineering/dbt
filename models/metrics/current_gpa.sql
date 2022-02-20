
WITH grades AS (

    SELECT
        *,
        {{ get_unweighted_gpa_point('letter_grade_earned') }} AS unweighted_gpa_point
    FROM {{ ref('rpt_student_section_grade') }} rpt_student_section_grade
    WHERE
        is_current_grading_period IS TRUE
        AND is_currently_enrolled_in_section = 'Yes'
        AND course_gpa_applicability = 'Applicable'

),

gpa_points AS (

    SELECT
        *,
        unweighted_gpa_point * available_credits AS unweighted_gpa_points
    FROM grades

)

SELECT
    school_name                                         AS school_name,
    student_unique_id                                   AS student_unique_id,
    student_display_name                                AS student_display_name,
    school_year                                         AS school_year,
    local_education_agency_name                         AS local_education_agency_name,
    is_actively_enrolled                                AS is_actively_enrolled,
    grade_level                                         AS grade_level,
    grade_level_id                                      AS grade_level_id,
    gender                                              AS gender,
    limited_english_proficiency                         AS limited_english_proficiency,
    is_english_language_learner                         AS is_english_language_learner,
    in_special_education_program                        AS in_special_education_program,
    is_hispanic                                         AS is_hispanic,
    race_and_ethnicity_roll_up                          AS race_and_ethnicity_roll_up,
    grading_period_description                          AS grading_period_description,
    SUM(unweighted_gpa_points) / SUM(available_credits) AS unweighted_current_gpa
FROM gpa_points
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
