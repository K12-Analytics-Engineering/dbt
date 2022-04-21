
select
    {{ dbt_utils.surrogate_key([
            'ssa.student_reference.student_unique_id',
            'ssa.school_year_type_reference.school_year'
    ]) }}                                                           as student_key,
    {{ dbt_utils.surrogate_key([
        'schools.local_education_agency_id',
        'ssa.school_year_type_reference.school_year'
    ]) }}                                                           as local_education_agency_key,
    {{ dbt_utils.surrogate_key([
        'ssa.school_reference.school_id',
        'ssa.school_year_type_reference.school_year'
    ]) }}                                                           as school_key,
    ssa.school_year_type_reference.school_year                      as school_year,
    {{ convert_grade_level_to_short_label('ssa.entry_grade_level_descriptor') }}     as grade_level,
    {{ convert_grade_level_to_id('ssa.entry_grade_level_descriptor') }}              as grade_level_id,
    ssa.entry_date                                                  as enrollment_date,
    ssa.entry_type_descriptor                                       as enrollment_type,
    ssa.exit_withdraw_date                                          as exit_date,
    ssa.exit_withdraw_type_descriptor                               as exit_type,
    ssa.primary_school                                              as is_primary_school,
    COUNT(calendar_dates.date)                                      as count_days_enrolled,
    if(
        ssa.exit_withdraw_date is null
        or (
            current_date >= ssa.entry_date
            and current_date < ssa.exit_withdraw_date
        ),
        1, 0)                                                       as is_actively_enrolled_in_school
from {{ ref('stg_edfi_student_school_associations') }} ssa
left join {{ ref('stg_edfi_schools') }} schools
    on ssa.school_reference.school_id = schools.school_id
    and ssa.school_year_type_reference.school_year = schools.school_year
left join {{ ref('stg_edfi_calendar_dates') }} calendar_dates
    on ssa.school_year = calendar_dates.school_year
    and ssa.school_reference.school_id = calendar_dates.calendar_reference.school_id
    and ssa.entry_date <= calendar_dates.date
    and (
        ssa.exit_withdraw_date is null
        or ssa.exit_withdraw_date > calendar_dates.date
    )
cross join unnest(calendar_dates.calendar_events) as calendar_events
where
    calendar_dates.date < current_date
    and calendar_events.calendar_event_descriptor = 'Instructional day'
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
