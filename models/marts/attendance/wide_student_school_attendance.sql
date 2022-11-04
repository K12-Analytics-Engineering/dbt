
with max_school_year_dates as (

    select
        school_year,
        max(date) as latest_date
    from {{ ref('fct_student_school_attendance') }}
    group by 1

)

select
    fct_student_school_attendance.school_year                                     as school_year,
    dim_school.lea_name                                 as lea_name,
    dim_school.school_id                                                   as school_id,
    dim_school.school_name                                                 as school_name,
    dim_student.student_unique_id                                          as student_unique_id,
    dim_student.student_display_name                                       as student_display_name,
    dim_student.student_last_surname                                       as student_last_surname,
    dim_student.student_first_name                                         as student_first_name,
    dim_student.is_actively_enrolled_in_school                             as is_actively_enrolled_in_school,
    dim_student.grade_level                                                as grade_level,
    dim_student.grade_level_id                                             as grade_level_id,
    dim_student.gender                                                     as gender,
    dim_student.limited_english_proficiency                                as limited_english_proficiency,
    dim_student.is_english_language_learner                                as is_english_language_learner,
    dim_student.in_special_education_program                               as in_special_education_program,
    dim_student.is_hispanic                                                as is_hispanic,
    dim_student.race_and_ethnicity_roll_up                                 as race_and_ethnicity_roll_up,
    dim_date.date                                                          as date,
    dim_date.month_name                                                    as month_name,
    dim_date.month_sort_order                                              as month_sort_order,
    fct_student_school_attendance.school_attendance_event_category         as school_attendance_event_category,
    fct_student_school_attendance.event_duration                                  as event_duration,
    fct_student_school_attendance.is_on_the_verge                                 as is_on_the_verge,
    fct_student_school_attendance.is_chronically_absent                           as is_chronically_absent,
    if(
        dim_date.date = max_school_year_dates.latest_date, true, false
    )                                                                      as is_latest_date_avaliable,
    rls_user_student_data_authorization.authorized_emails
from {{ ref('fct_student_school_attendance') }} fct_student_school_attendance
left join {{ ref('dim_student') }} dim_student
    on fct_student_school_attendance.student_key = dim_student.student_key
left join {{ ref('dim_date') }} dim_date
    on fct_student_school_attendance.date = dim_date.date
left join {{ ref('dim_school') }} dim_school
    on fct_student_school_attendance.school_key = dim_school.school_key
left join {{ ref('rls_user_student_data_authorization') }} rls_user_student_data_authorization
    on fct_student_school_attendance.student_key = rls_user_student_data_authorization.student_key
left join max_school_year_dates on fct_student_school_attendance.school_year = max_school_year_dates.school_year
