

WITH student_attendance AS (

    SELECT
        {{ dbt_utils.surrogate_key([
            'schools.local_education_agency_id',
            'ssa.school_year_type_reference.school_year'
        ]) }}                                                                                               AS local_education_agency_key,
        {{ dbt_utils.surrogate_key([
            'ssa.school_reference.school_id',
            'ssa.school_year_type_reference.school_year'
        ]) }}                                                                                               AS school_key,
        {{ dbt_utils.surrogate_key([
            'ssa.student_reference.student_unique_id',
            'ssa.school_year_type_reference.school_year'
        ]) }}                                                                                               AS student_key,
        ssa.school_year_type_reference.school_year                                                          AS school_year,
        calendar_dates.date                                                                                 AS date,
        IFNULL(MIN(school_attendance.attendance_event_category_descriptor), 'In Attendance')                AS school_attendance_event_category_descriptor,
        IFNULL(school_attendance.event_duration, 0)                                                         AS event_duration,
        MAX(IF(school_attendance.attendance_event_category_descriptor = 'In Attendance', 1, 0))             AS reported_as_present_at_school,
        MAX(IF(
            school_attendance.attendance_event_category_descriptor IN ('Excused Absence', 'Unexcused Absence'), 1, 0
        ))                                                                                                  AS reported_as_absent_from_school,
        MAX(IF(
            school_attendance.attendance_event_category_descriptor = 'In Attendance' 
                AND student_section_associations.homeroom_indicator IS TRUE,
            1, 0
        ))                                                                                                  AS reported_as_present_at_home_room,
        MAX(IF(
            school_attendance.attendance_event_category_descriptor IN ('Excused Absence', 'Unexcused Absence') 
                AND student_section_associations.homeroom_indicator IS TRUE,
            1, 0
        ))                                                                                                  AS reported_as_absent_from_home_room,
        COUNT(1) OVER(
            PARTITION BY ssa.school_year_type_reference.school_year, ssa.student_reference.student_unique_id
            ORDER BY ssa.school_year_type_reference.school_year, ssa.student_reference.student_unique_id, calendar_dates.date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )                                                                                                   AS number_days_enrolled_thus_far,
        SUM(school_attendance.event_duration) OVER(
            PARTITION BY ssa.school_year_type_reference.school_year, ssa.student_reference.student_unique_id
            ORDER BY ssa.school_year_type_reference.school_year, ssa.student_reference.student_unique_id, calendar_dates.date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )                                                                                                   AS sum_event_duration_thus_far
    FROM {{ ref('stg_edfi_student_school_associations') }} ssa
    LEFT JOIN {{ ref('stg_edfi_schools') }} schools
        ON ssa.school_reference.school_id = schools.school_id
        AND ssa.school_year_type_reference.school_year = schools.school_year
    LEFT JOIN {{ ref('stg_edfi_students') }} students
        ON ssa.school_year = students.school_year
        AND ssa.student_reference.student_unique_id = students.student_unique_id
    LEFT JOIN {{ ref('stg_edfi_calendar_dates') }} calendar_dates
        ON ssa.school_year = calendar_dates.school_year
        AND ssa.school_reference.school_id = calendar_dates.calendar_reference.school_id
        AND ssa.entry_date <= calendar_dates.date
        AND (
            ssa.exit_withdraw_date IS NULL
            OR ssa.exit_withdraw_date >= calendar_dates.date
        )
    CROSS JOIN UNNEST(calendar_dates.calendar_events) AS calendar_events
    -- school attendance
    LEFT JOIN {{ ref('stg_edfi_student_school_attendance_events') }} school_attendance
        ON ssa.school_year = school_attendance.school_year
        AND school_attendance.student_reference.student_unique_id = ssa.student_reference.student_unique_id
        AND school_attendance.school_reference.school_id = ssa.school_reference.school_id
        AND (
            ssa.school_year_type_reference.school_year IS NULL
            OR 
            school_attendance.session_reference.school_year = ssa.school_year_type_reference.school_year
        )
        AND school_attendance.event_date = calendar_dates.date
    -- section attendance
    LEFT JOIN{{ ref('stg_edfi_student_section_attendance_events') }} section_attendance
        ON ssa.school_year = section_attendance.school_year
        AND section_attendance.student_reference.student_unique_id = ssa.student_reference.student_unique_id
        AND section_attendance.section_reference.school_id = ssa.school_reference.school_id
        AND section_attendance.event_date = calendar_dates.date
        AND (
            ssa.school_year_type_reference.school_year IS NULL
            OR 
            section_attendance.section_reference.school_year = ssa.school_year_type_reference.school_year
        )
    LEFT JOIN {{ ref('stg_edfi_student_section_associations') }} student_section_associations
        ON section_attendance.school_year = student_section_associations.school_year
        AND student_section_associations.student_reference.student_unique_id = section_attendance.student_reference.student_unique_id
        AND student_section_associations.section_reference.local_course_code = section_attendance.section_reference.local_course_code
        AND student_section_associations.section_reference.school_id = section_attendance.section_reference.school_id
        AND student_section_associations.section_reference.school_year = section_attendance.section_reference.school_year
        AND student_section_associations.section_reference.section_identifier = section_attendance.section_reference.section_identifier
        AND student_section_associations.section_reference.session_name = section_attendance.section_reference.session_name
    WHERE
        calendar_dates.date < CURRENT_DATE
        AND calendar_events.calendar_event_descriptor = 'Instructional day'
    GROUP BY
        ssa.student_reference.student_unique_id,
        schools.local_education_agency_id,
        ssa.school_reference.school_id,
        ssa.school_year_type_reference.school_year,
        calendar_dates.date,
        school_attendance.event_duration

)

SELECT
    local_education_agency_key,
    school_key,
    student_key,
    school_year,
    date,
    school_attendance_event_category_descriptor,
    event_duration,
    reported_as_present_at_school,
    reported_as_absent_from_school,
    reported_as_present_at_home_room,
    reported_as_absent_from_home_room,
    IF(sum_event_duration_thus_far >= 15, 1, 0)                                                                    AS is_chronically_absent,
    IF((number_days_enrolled_thus_far - sum_event_duration_thus_far) / number_days_enrolled_thus_far < 0.92, 1, 0) AS is_on_the_verge -- early warning indicator
FROM student_attendance
