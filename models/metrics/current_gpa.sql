
WITH grades AS (

    SELECT
        fct_student_section_grade.student_key,
        dim_section.available_credits,
        {{ get_unweighted_gpa_point('letter_grade_earned') }} AS unweighted_gpa_point
    FROM {{ ref('fct_student_section_grade') }} fct_student_section_grade
    LEFT JOIN {{ ref('dim_grading_period') }} dim_grading_period
        ON fct_student_section_grade.grading_period_key = dim_grading_period.grading_period_key
    LEFT JOIN {{ ref('dim_section') }} dim_section
        ON fct_student_section_grade.section_key = dim_section.section_key
    WHERE
        dim_grading_period.is_current_grading_period IS TRUE
        AND CURRENT_DATE BETWEEN dim_section.start_date AND dim_section.end_date
        AND course_gpa_applicability = 'Applicable'

),

gpa_points AS (

    SELECT
        student_key,
        available_credits,
        unweighted_gpa_point * available_credits AS unweighted_gpa_points
    FROM grades

)

SELECT
    dim_school.school_name                                           AS school_name,
    dim_student. student_unique_id                                   AS student_unique_id,
    dim_student.student_display_name                                 AS student_display_name,
    fct_student_school.school_year                                   AS school_year,
    dim_student.grade_level                                          AS grade_level,
    dim_student.grade_level_id                                       AS grade_level_id,
    dim_student.gender                                               AS gender,
    dim_student.limited_english_proficiency                          AS limited_english_proficiency,
    dim_student.is_english_language_learner                          AS is_english_language_learner,
    dim_student.in_special_education_program                         AS in_special_education_program,
    dim_student.is_hispanic                                          AS is_hispanic,
    dim_student.race_and_ethnicity_roll_up                           AS race_and_ethnicity_roll_up,
    SUM(unweighted_gpa_points) / SUM(available_credits) AS unweighted_current_gpa
FROM gpa_points
LEFT JOIN {{ ref('fct_student_school') }} fct_student_school
    ON gpa_points.student_key = fct_student_school.student_key
LEFT JOIN {{ ref('dim_student') }} dim_student
    ON gpa_points.student_key = dim_student.student_key
LEFT JOIN {{ ref('dim_school') }} dim_school
    ON fct_student_school.school_key = dim_school.school_key
WHERE fct_student_school.is_actively_enrolled = 1
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
