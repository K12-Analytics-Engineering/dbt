
with grades as (

    select
        fct_student_grade.student_key,
        dim_section.available_credits,
        {{ get_unweighted_gpa_point('letter_grade_earned') }} as unweighted_gpa_point
    from {{ ref('fct_student_grade') }} fct_student_grade
    left join {{ ref('dim_grading_period') }} dim_grading_period
        on fct_student_grade.grading_period_key = dim_grading_period.grading_period_key
    left join {{ ref('dim_section') }} dim_section
        on fct_student_grade.section_key = dim_section.section_key
    where
        dim_grading_period.is_current_grading_period is true
        and fct_student_grade.is_actively_enrolled_in_section = 1
        and course_gpa_applicability = 'Applicable'

),

gpa_points as (

    select
        student_key,
        available_credits,
        unweighted_gpa_point * available_credits as unweighted_gpa_points
    from grades

)

select
    dim_school.school_name                                           as school_name,
    dim_student.student_unique_id                                    as student_unique_id,
    dim_student.student_display_name                                 as student_display_name,
    fct_student_school.school_year                                   as school_year,
    dim_student.grade_level                                          as grade_level,
    dim_student.grade_level_id                                       as grade_level_id,
    dim_student.gender                                               as gender,
    dim_student.limited_english_proficiency                          as limited_english_proficiency,
    dim_student.is_english_language_learner                          as is_english_language_learner,
    dim_student.in_special_education_program                         as in_special_education_program,
    dim_student.is_hispanic                                          as is_hispanic,
    dim_student.race_and_ethnicity_roll_up                           as race_and_ethnicity_roll_up,
    SUM(unweighted_gpa_points) / SUM(available_credits) as unweighted_current_gpa
from gpa_points
left join {{ ref('fct_student_school') }} fct_student_school
    on gpa_points.student_key = fct_student_school.student_key
left join {{ ref('dim_student') }} dim_student
    on gpa_points.student_key = dim_student.student_key
left join {{ ref('dim_school') }} dim_school
    on fct_student_school.school_key = dim_school.school_key
where fct_student_school.is_actively_enrolled_in_school = 1
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
