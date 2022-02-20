
SELECT
    {{ dbt_utils.surrogate_key([
        'sessions.school_reference.school_id',
        'sessions.school_year_type_reference.school_year',
        'sessions.session_name'
    ]) }}                                                                                   AS session_key,
    {{ dbt_utils.surrogate_key([
        'sessions.school_reference.school_id',
        'sessions.school_year_type_reference.school_year'
    ]) }}                                                                                   AS school_key,
    sessions.school_year_type_reference.school_year                                         AS school_year,
    school_year_types.school_year_description                                               AS school_year_name,
    sessions.session_name                                                                   AS session_name,
    sessions.term_descriptor                                                                AS term_name,
    sessions.total_instructional_days                                                       AS total_instructional_days,
    sessions.begin_date                                                                     AS session_begin_date,
    sessions.end_date                                                                       AS session_end_date
FROM {{ ref('stg_edfi_sessions') }} sessions
LEFT JOIN {{ ref('stg_edfi_school_year_types') }} school_year_types
    ON sessions.school_year_type_reference.school_year = school_year_types.school_year
