{{
  config(
    cluster_by = ["student_display_name", "date"]
  )
}}


WITH max_school_year_dates AS (

    SELECT
        school_year,
        MAX(date) AS latest_date
    FROM {{ ref('fct_student_attendance') }}
    GROUP BY 1

)

SELECT
    fct_student_attendance.school_year                                     AS school_year,
    dim_local_education_agency.local_education_agency_name                 AS local_education_agency_name,
    dim_school.school_id                                                   AS school_id,
    dim_school.school_name                                                 AS school_name,
    dim_student.student_unique_id                                          AS student_unique_id,
    dim_student.student_display_name                                       AS student_display_name,
    dim_student.student_last_surname                                       AS student_last_surname,
    dim_student.student_first_name                                         AS student_first_name,
    dim_student.is_actively_enrolled_in_school                             AS is_actively_enrolled_in_school,
    dim_student.grade_level                                                AS grade_level,
    dim_student.grade_level_id                                             AS grade_level_id,
    dim_student.gender                                                     AS gender,
    dim_student.limited_english_proficiency                                AS limited_english_proficiency,
    dim_student.is_english_language_learner                                AS is_english_language_learner,
    dim_student.in_special_education_program                               AS in_special_education_program,
    dim_student.is_hispanic                                                AS is_hispanic,
    dim_student.race_and_ethnicity_roll_up                                 AS race_and_ethnicity_roll_up,
    dim_date.date                                                          AS date,
    dim_date.month_name                                                    AS month_name,
    dim_date.month_sort_order                                              AS month_sort_order,
    fct_student_attendance.school_attendance_event_category_descriptor     AS school_attendance_event_category_descriptor,
    fct_student_attendance.event_duration                                  AS event_duration,
    fct_student_attendance.reported_as_present_at_school                   AS reported_as_present_at_school,
    fct_student_attendance.reported_as_absent_from_school                  AS reported_as_absent_from_school,
    fct_student_attendance.reported_as_present_at_home_room                AS reported_as_present_at_home_room,
    fct_student_attendance.reported_as_absent_from_home_room               AS reported_as_absent_from_home_room,
    fct_student_attendance.is_on_the_verge                                 AS is_on_the_verge,
    fct_student_attendance.is_chronically_absent                           AS is_chronically_absent,
    IF(
        dim_date.date = max_school_year_dates.latest_date, TRUE, FALSE
    )                                                                      AS is_latest_date_avaliable
FROM {{ ref('fct_student_attendance') }} fct_student_attendance
LEFT JOIN {{ ref('dim_student') }} dim_student
    ON fct_student_attendance.student_key = dim_student.student_key
LEFT JOIN {{ ref('dim_date') }} dim_date
    ON fct_student_attendance.date = dim_date.date
LEFT JOIN {{ ref('dim_school') }} dim_school
    ON fct_student_attendance.school_key = dim_school.school_key
LEFT JOIN {{ ref('dim_local_education_agency') }} dim_local_education_agency
    ON dim_school.local_education_agency_key = dim_local_education_agency.local_education_agency_key
LEFT JOIN max_school_year_dates ON fct_student_attendance.school_year = max_school_year_dates.school_year
