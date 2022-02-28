

WITH section_grade AS (

    SELECT
        fct_student_section_grade.school_year,
        fct_student_section_grade.school_key,
        fct_student_section_grade.section_key,
        fct_student_section_grade.student_key,
        fct_student_section_grade.staff_group_key,
        fct_student_section_grade.is_actively_enrolled_in_section,
        ARRAY_AGG(
            STRUCT(
                dim_grading_period.grading_period_name,
                dim_grading_period.is_current_grading_period,
                fct_student_section_grade.grade_type,
                fct_student_section_grade.numeric_grade_earned,
                fct_student_section_grade.letter_grade_earned
            )
        ) AS grade
    FROM {{ ref('fct_student_section_grade') }} fct_student_section_grade
    LEFT JOIN {{ ref('dim_grading_period') }} dim_grading_period
        ON fct_student_section_grade.grading_period_key = dim_grading_period.grading_period_key
    GROUP BY 1,2,3,4,5,6

),

staff AS (

    SELECT
        section_grade.school_year,
        section_grade.school_key,
        section_grade.section_key,
        section_grade.student_key,
        ARRAY_AGG(
            STRUCT(
                dim_staff.staff_last_surname,
                dim_staff.staff_first_name,
                dim_staff.staff_display_name,
                dim_staff.email,
                bridge_staff_group.classroom_position
            )
        ) AS staff
    FROM section_grade
    LEFT JOIN {{ ref('bridge_staff_group') }} bridge_staff_group
        ON section_grade.staff_group_key = bridge_staff_group.staff_group_key
    LEFT JOIN {{ ref('dim_staff') }} dim_staff
        ON bridge_staff_group.staff_key = dim_staff.staff_key
    GROUP BY 1,2,3,4

)


SELECT
    section_grade.school_year                                   AS school_year,
    dim_local_education_agency.local_education_agency_name      AS local_education_agency_name,
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
    dim_section.local_course_code                               AS local_course_code,
    dim_section.course_title                                    AS course_title,
    dim_section.section_identifier                              AS section_identifier,
    dim_section.course_academic_subject                         AS academic_subject,
    dim_section.course_gpa_applicability                        AS course_gpa_applicability,
    dim_section.available_credits                               AS available_credits,
    section_grade.is_actively_enrolled_in_section               AS is_actively_enrolled_in_section,
    dim_session.session_name                                    AS session_name,
    dim_session.term_name                                       AS term_name,
    staff.staff                                                 AS staff,
    section_grade.grade                                         AS grade
FROM section_grade
LEFT JOIN staff
    ON section_grade.school_year = staff.school_year
    AND section_grade.school_key = staff.school_key
    AND section_grade.section_key = staff.section_key
    AND section_grade.student_key = staff.student_key
LEFT JOIN {{ ref('dim_section') }} dim_section
    ON section_grade.section_key = dim_section.section_key
LEFT JOIN {{ ref('dim_session') }} dim_session
    ON dim_section.session_key = dim_session.session_key
LEFT JOIN {{ ref('dim_student') }} dim_student
    ON section_grade.student_key = dim_student.student_key
LEFT JOIN {{ ref('dim_school') }} dim_school
    ON section_grade.school_key = dim_school.school_key
LEFT JOIN {{ ref('dim_local_education_agency') }} dim_local_education_agency
    ON dim_school.local_education_agency_key = dim_local_education_agency.local_education_agency_key
