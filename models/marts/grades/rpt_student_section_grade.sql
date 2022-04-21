

with section_grade as (

    select
        fct_student_grade.school_year,
        fct_student_grade.school_key,
        fct_student_grade.section_key,
        fct_student_grade.student_key,
        fct_student_grade.staff_group_key,
        fct_student_grade.is_actively_enrolled_in_section,
        ARRAY_AGG(
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
    group by 1,2,3,4,5,6

),

staff as (

    select
        section_grade.school_year,
        section_grade.school_key,
        section_grade.section_key,
        section_grade.student_key,
        ARRAY_AGG(
            struct(
                dim_staff.staff_last_surname,
                dim_staff.staff_first_name,
                dim_staff.staff_display_name,
                dim_staff.email,
                bridge_staff_group.classroom_position
            )
        ) as staff
    from section_grade
    left join {{ ref('bridge_staff_group') }} bridge_staff_group
        on section_grade.staff_group_key = bridge_staff_group.staff_group_key
    left join {{ ref('dim_staff') }} dim_staff
        on bridge_staff_group.staff_key = dim_staff.staff_key
    group by 1,2,3,4

)


select
    section_grade.school_year                                   as school_year,
    dim_local_education_agency.local_education_agency_name      as local_education_agency_name,
    dim_school.school_name                                      as school_name,
    dim_student.student_unique_id                               as student_unique_id,
    dim_student.student_last_surname                            as student_last_surname,
    dim_student.student_first_name                              as student_first_name,
    dim_student.student_display_name                            as student_display_name,
    dim_student.is_actively_enrolled_in_school                  as is_actively_enrolled_in_school,
    dim_student.grade_level                                     as grade_level,
    dim_student.grade_level_id                                  as grade_level_id,
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
    dim_session.session_name                                    as session_name,
    dim_session.term_name                                       as term_name,
    staff.staff                                                 as staff,
    section_grade.grade                                         as grade
from section_grade
left join staff
    on section_grade.school_year = staff.school_year
    and section_grade.school_key = staff.school_key
    and section_grade.section_key = staff.section_key
    and section_grade.student_key = staff.student_key
left join {{ ref('dim_section') }} dim_section
    on section_grade.section_key = dim_section.section_key
left join {{ ref('dim_session') }} dim_session
    on dim_section.session_key = dim_session.session_key
left join {{ ref('dim_student') }} dim_student
    on section_grade.student_key = dim_student.student_key
left join {{ ref('dim_school') }} dim_school
    on section_grade.school_key = dim_school.school_key
left join {{ ref('dim_local_education_agency') }} dim_local_education_agency
    on dim_school.local_education_agency_key = dim_local_education_agency.local_education_agency_key
