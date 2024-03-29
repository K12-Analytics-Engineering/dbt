
with section_grade as (

    select
        fct_student_grade.school_year,
        fct_student_grade.school_key,
        fct_student_grade.section_key,
        fct_student_grade.student_key,
        fct_student_grade.staff_group_key,
        dim_grading_period.session_name,
        dim_grading_period.term_name,
        fct_student_grade.is_actively_enrolled_in_section,
        array_agg(
            struct(
                dim_grading_period.grading_period_name,
                dim_grading_period.is_current_grading_period,
                fct_student_grade.grade_type,
                fct_student_grade.numeric_grade_earned,
                fct_student_grade.letter_grade_earned
            )
        ) as grade
    from {{ ref('fct_student_grade') }} fct_student_grade
    left join {{ ref('dim_grading_period') }} dim_grading_period
        on fct_student_grade.grading_period_key = dim_grading_period.grading_period_key
    {{ dbt_utils.group_by(n=8) }}

),

staff as (

    select
        section_grade.school_year,
        section_grade.school_key,
        section_grade.section_key,
        section_grade.student_key,
        array_agg(
            struct(
                dim_staff.staff_last_surname,
                dim_staff.staff_first_name,
                dim_staff.staff_display_name,
                dim_staff.staff_email,
                bridge_staff_group.classroom_position
            )
        ) as staff
    from section_grade
    left join {{ ref('bridge_staff_group') }} bridge_staff_group
        on section_grade.staff_group_key = bridge_staff_group.staff_group_key
    left join {{ ref('dim_staff') }} dim_staff
        on bridge_staff_group.staff_key = dim_staff.staff_key
    {{ dbt_utils.group_by(n=4) }}

),

student_enrollment as (

    select
        fct_student_school.school_key,
        fct_student_school.student_key,
        fct_student_school.grade_level,
        fct_student_school.grade_level_id,
        fct_student_school.is_actively_enrolled_in_school
    from {{ ref('fct_student_school') }} fct_student_school
    qualify row_number() over (
        partition by school_key, student_key
        order by enrollment_date desc
    ) = 1

)


select
    section_grade.school_year                                   as school_year,
    dim_school.lea_name                                         as lea_name,
    dim_school.school_name                                      as school_name,
    dim_student.student_unique_id                               as student_unique_id,
    dim_student.student_last_surname                            as student_last_surname,
    dim_student.student_first_name                              as student_first_name,
    dim_student.student_display_name                            as student_display_name,
    student_enrollment.is_actively_enrolled_in_school           as is_actively_enrolled_in_school,
    student_enrollment.grade_level                              as grade_level,
    student_enrollment.grade_level_id                           as grade_level_id,
    dim_student.gender                                          as gender,
    dim_student.limited_english_proficiency                     as limited_english_proficiency,
    dim_student.is_english_language_learner                     as is_english_language_learner,
    dim_student.in_special_education_program                    as in_special_education_program,
    dim_student.is_hispanic                                     as is_hispanic,
    dim_student.race_and_ethnicity_roll_up                      as race_and_ethnicity_roll_up,
    dim_section.local_course_code                               as local_course_code,
    dim_section.course_title                                    as course_title,
    dim_section.section_identifier                              as section_identifier,
    dim_section.course_academic_subject                         as academic_subject,
    dim_section.course_gpa_applicability                        as course_gpa_applicability,
    dim_section.available_credits                               as available_credits,
    section_grade.is_actively_enrolled_in_section               as is_actively_enrolled_in_section,
    section_grade.session_name                                  as session_name,
    section_grade.term_name                                     as term_name,
    staff.staff                                                 as staff,
    section_grade.grade                                         as grade
from section_grade
left join student_enrollment
    on section_grade.school_key = student_enrollment.school_key
    and section_grade.student_key = student_enrollment.student_key
left join staff
    on section_grade.school_year = staff.school_year
    and section_grade.school_key = staff.school_key
    and section_grade.section_key = staff.section_key
    and section_grade.student_key = staff.student_key
left join {{ ref('dim_section') }} dim_section
    on section_grade.section_key = dim_section.section_key
left join {{ ref('dim_student') }} dim_student
    on section_grade.student_key = dim_student.student_key
left join {{ ref('dim_school') }} dim_school
    on section_grade.school_key = dim_school.school_key
