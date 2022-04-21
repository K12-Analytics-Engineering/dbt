
select
    {{ dbt_utils.surrogate_key([
        'sessions.school_reference.school_id',
        'sessions.school_year_type_reference.school_year',
        'sessions.session_name'
    ]) }}                                                                                   as session_key,
    {{ dbt_utils.surrogate_key([
        'sessions.school_reference.school_id',
        'sessions.school_year_type_reference.school_year'
    ]) }}                                                                                   as school_key,
    sessions.school_year_type_reference.school_year                                         as school_year,
    school_year_types.school_year_description                                               as school_year_name,
    sessions.session_name                                                                   as session_name,
    sessions.term_descriptor                                                                as term_name,
    sessions.total_instructional_days                                                       as total_instructional_days,
    sessions.begin_date                                                                     as session_begin_date,
    sessions.end_date                                                                       as session_end_date
from {{ ref('stg_edfi_sessions') }} sessions
left join {{ ref('stg_edfi_school_year_types') }} school_year_types
    on sessions.school_year_type_reference.school_year = school_year_types.school_year
