

with student_attendance as (

    select
        {{ dbt_utils.surrogate_key([
            'schools.local_education_agency_id',
            'ssa.school_year_type_reference.school_year'
        ]) }}                                                                                               as local_education_agency_key,
        {{ dbt_utils.surrogate_key([
            'ssa.school_reference.school_id',
            'ssa.school_year_type_reference.school_year'
        ]) }}                                                                                               as school_key,
        {{ dbt_utils.surrogate_key([
            'ssa.student_reference.student_unique_id',
            'ssa.school_year_type_reference.school_year'
        ]) }}                                                                                               as student_key,
        ssa.school_year_type_reference.school_year                                                          as school_year,
        calendar_dates.date                                                                                 as date,
        ifnull(MIN(school_attendance.attendance_event_category_descriptor), 'In Attendance')                as school_attendance_event_category_descriptor,
        ifnull(school_attendance.event_duration, 0)                                                         as event_duration,
        max(if(school_attendance.attendance_event_category_descriptor = 'In Attendance', 1, 0))             as reported_as_present_at_school,
        max(if(
            school_attendance.attendance_event_category_descriptor in ('Excused Absence', 'Unexcused Absence'), 1, 0
        ))                                                                                                  as reported_as_absent_from_school,
        max(if(
            school_attendance.attendance_event_category_descriptor = 'In Attendance' 
                and student_section_associations.homeroom_indicator is true,
            1, 0
        ))                                                                                                  as reported_as_present_at_home_room,
        max(if(
            school_attendance.attendance_event_category_descriptor in ('Excused Absence', 'Unexcused Absence') 
                and student_section_associations.homeroom_indicator is true,
            1, 0
        ))                                                                                                  as reported_as_absent_from_home_room,
        COUNT(1) over(
            partition by ssa.school_year_type_reference.school_year, ssa.student_reference.student_unique_id
            order by ssa.school_year_type_reference.school_year, ssa.student_reference.student_unique_id, calendar_dates.date 
            rows between unbounded preceding and current row
        )                                                                                                   as number_days_enrolled_thus_far,
        SUM(school_attendance.event_duration) over(
            partition by ssa.school_year_type_reference.school_year, ssa.student_reference.student_unique_id
            order by ssa.school_year_type_reference.school_year, ssa.student_reference.student_unique_id, calendar_dates.date 
            rows between unbounded preceding and current row
        )                                                                                                   as sum_event_duration_thus_far
    from {{ ref('stg_edfi_student_school_associations') }} ssa
    left join {{ ref('stg_edfi_schools') }} schools
        on ssa.school_reference.school_id = schools.school_id
        and ssa.school_year_type_reference.school_year = schools.school_year
    left join {{ ref('stg_edfi_students') }} students
        on ssa.school_year = students.school_year
        and ssa.student_reference.student_unique_id = students.student_unique_id
    left join {{ ref('stg_edfi_calendar_dates') }} calendar_dates
        on ssa.school_year = calendar_dates.school_year
        and ssa.school_reference.school_id = calendar_dates.calendar_reference.school_id
        and ssa.entry_date <= calendar_dates.date
        and (
            ssa.exit_withdraw_date is null
            or ssa.exit_withdraw_date >= calendar_dates.date
        )
    cross join unnest(calendar_dates.calendar_events) as calendar_events
    -- school attendance
    left join {{ ref('stg_edfi_student_school_attendance_events') }} school_attendance
        on ssa.school_year = school_attendance.school_year
        and school_attendance.student_reference.student_unique_id = ssa.student_reference.student_unique_id
        and school_attendance.school_reference.school_id = ssa.school_reference.school_id
        and (
            ssa.school_year_type_reference.school_year is null
            or 
            school_attendance.session_reference.school_year = ssa.school_year_type_reference.school_year
        )
        and school_attendance.event_date = calendar_dates.date
    -- section attendance
    left join{{ ref('stg_edfi_student_section_attendance_events') }} section_attendance
        on ssa.school_year = section_attendance.school_year
        and section_attendance.student_reference.student_unique_id = ssa.student_reference.student_unique_id
        and section_attendance.section_reference.school_id = ssa.school_reference.school_id
        and section_attendance.event_date = calendar_dates.date
        and (
            ssa.school_year_type_reference.school_year is null
            or 
            section_attendance.section_reference.school_year = ssa.school_year_type_reference.school_year
        )
    left join {{ ref('stg_edfi_student_section_associations') }} student_section_associations
        on section_attendance.school_year = student_section_associations.school_year
        and student_section_associations.student_reference.student_unique_id = section_attendance.student_reference.student_unique_id
        and student_section_associations.section_reference.local_course_code = section_attendance.section_reference.local_course_code
        and student_section_associations.section_reference.school_id = section_attendance.section_reference.school_id
        and student_section_associations.section_reference.school_year = section_attendance.section_reference.school_year
        and student_section_associations.section_reference.section_identifier = section_attendance.section_reference.section_identifier
        and student_section_associations.section_reference.session_name = section_attendance.section_reference.session_name
    where
        calendar_dates.date < current_date
        and calendar_events.calendar_event_descriptor = 'Instructional day'
    group by
        ssa.student_reference.student_unique_id,
        schools.local_education_agency_id,
        ssa.school_reference.school_id,
        ssa.school_year_type_reference.school_year,
        calendar_dates.date,
        school_attendance.event_duration

)

select
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
    if(sum_event_duration_thus_far >= 15, 1, 0)                                                                    as is_chronically_absent,
    if((number_days_enrolled_thus_far - sum_event_duration_thus_far) / number_days_enrolled_thus_far < 0.92, 1, 0) as is_on_the_verge -- early warning indicator
from student_attendance
