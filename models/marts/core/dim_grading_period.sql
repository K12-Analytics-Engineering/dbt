
with unique_records as (

    select distinct
        student_section_association_reference.session_name,
        grading_period_reference.school_id,
        grading_period_reference.school_year,
        grading_period_reference.grading_period_name,
        grading_period_reference.period_sequence
    from {{ ref('stg_edfi_grades') }} grades

),

grades_grading_periods_unioned as (

    select
        {{ dbt_utils.surrogate_key([
            'unique_records.school_id',
            'unique_records.school_year',
            'unique_records.session_name',
            'unique_records.grading_period_name',
            'unique_records.period_sequence'
        ]) }}                                               as grading_period_key,
        unique_records.session_name                         as session_name,
        sessions.term_descriptor                            as term_name,
        unique_records.school_year                          as school_year,
        grading_periods.grading_period_name                 as grading_period_name,
        grading_periods.period_sequence                     as period_sequence,
        grading_periods.begin_date                          as grading_period_begin_date,
        grading_periods.end_date                            as grading_period_end_date,
        grading_periods.total_instructional_days            as total_instructional_days
    from unique_records
    left join {{ ref('stg_edfi_grading_periods') }} grading_periods
        on unique_records.school_id = grading_periods.school_reference.school_id
        and unique_records.school_year = grading_periods.school_year_type_reference.school_year
        and unique_records.grading_period_name = grading_periods.grading_period_name
        and unique_records.period_sequence = grading_periods.period_sequence
    left join {{ ref('stg_edfi_sessions') }} sessions
        on unique_records.school_id = sessions.school_reference.school_id
        and unique_records.school_year = sessions.school_year_type_reference.school_year
        and unique_records.session_name = sessions.session_name


    union all


    select
        {{ dbt_utils.surrogate_key([
            'grading_periods.school_reference.school_id',
            'grading_periods.school_year_type_reference.school_year',
            'sessions.session_name',
            'grading_periods.grading_period_name',
            'grading_periods.period_sequence'
        ]) }}                                                                                   as grading_period_key,
        sessions.session_name                                                                   as session_name,
        sessions.term_descriptor                                                                as term_name,
        sessions.school_year_type_reference.school_year                                         as school_year,
        grading_periods.grading_period_name                                                     as grading_period_name,
        grading_periods.period_sequence                                                         as period_sequence,
        grading_periods.begin_date                                                              as grading_period_begin_date,
        grading_periods.end_date                                                                as grading_period_end_date,
        grading_periods.total_instructional_days                                                as total_instructional_day,
    from {{ ref('stg_edfi_sessions') }} sessions
    left join unnest(sessions.grading_periods) sessions_grading_periods
    left join {{ ref('stg_edfi_school_year_types') }} school_year_types
        on sessions.school_year_type_reference.school_year = school_year_types.school_year
    left join {{ ref('stg_edfi_grading_periods') }} grading_periods
        on sessions.school_year_type_reference.school_year = grading_periods.school_year
        and sessions_grading_periods.grading_period_reference.grading_period_name = grading_periods.grading_period_name
        and sessions_grading_periods.grading_period_reference.period_sequence = grading_periods.period_sequence
        and sessions_grading_periods.grading_period_reference.school_id = grading_periods.school_reference.school_id
    where sessions_grading_periods.grading_period_reference.grading_period_name != ''

)

select distinct
    grading_period_key,
    school_year,
    session_name,
    term_name,
    grading_period_name,
    period_sequence,
    grading_period_begin_date,
    grading_period_end_date,
    total_instructional_days,
    if(
        current_date between grading_period_begin_date and grading_period_end_date,
        true,
        false
    )                                                                 as is_current_grading_period
from grades_grading_periods_unioned
