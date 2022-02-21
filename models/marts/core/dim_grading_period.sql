{{
  config(
    labels = {'analytics_middle_tier': 'yes'}
  )
}}


WITH unique_records AS (
    SELECT DISTINCT
        student_section_association_reference.session_name,
        grading_period_reference.school_id,
        grading_period_reference.school_year,
        grading_period_reference.grading_period_descriptor,
        grading_period_reference.period_sequence
    FROM {{ ref('stg_edfi_grades') }} grades

),

grades_grading_periods_unioned AS (

    SELECT
        {{ dbt_utils.surrogate_key([
            'unique_records.school_id',
            'unique_records.school_year',
            'unique_records.session_name',
            'unique_records.grading_period_descriptor',
            'unique_records.period_sequence'
        ]) }}                                               AS grading_period_key,
        {{ dbt_utils.surrogate_key([
            'unique_records.school_id',
            'unique_records.school_year',
            'unique_records.session_name'
        ]) }}                                               AS session_key,
        {{ dbt_utils.surrogate_key([
            'unique_records.school_id',
            'unique_records.school_year'
        ]) }}                                               AS school_key,
        unique_records.school_year                          AS school_year,
        grading_periods.grading_period_descriptor           AS grading_period_name,
        grading_periods.period_sequence                     AS period_sequence,
        grading_periods.begin_date                          AS grading_period_begin_date,
        grading_periods.end_date                            AS grading_period_end_date,
        grading_periods.total_instructional_days            AS total_instructional_days
    FROM unique_records
    LEFT JOIN {{ ref('stg_edfi_grading_periods') }} grading_periods
        ON unique_records.school_id = grading_periods.school_reference.school_id
        AND unique_records.school_year = grading_periods.school_year_type_reference.school_year
        AND unique_records.grading_period_descriptor = grading_periods.grading_period_descriptor
        AND unique_records.period_sequence = grading_periods.period_sequence


    UNION ALL


    SELECT
        {{ dbt_utils.surrogate_key([
            'grading_periods.school_reference.school_id',
            'grading_periods.school_year_type_reference.school_year',
            'sessions.session_name',
            'grading_periods.grading_period_descriptor',
            'grading_periods.period_sequence'
        ]) }}                                                                                   AS grading_period_key,
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
        grading_periods.grading_period_descriptor                                               AS grading_period_name,
        grading_periods.period_sequence                                                         AS period_sequence,
        grading_periods.begin_date                                                              AS grading_period_begin_date,
        grading_periods.end_date                                                                AS grading_period_end_date,
        grading_periods.total_instructional_days                                                AS total_instructional_day,
    FROM {{ ref('stg_edfi_sessions') }} sessions
    LEFT JOIN UNNEST(sessions.grading_periods) sessions_grading_periods
    LEFT JOIN {{ ref('stg_edfi_school_year_types') }} school_year_types
        ON sessions.school_year_type_reference.school_year = school_year_types.school_year
    LEFT JOIN {{ ref('stg_edfi_grading_periods') }} grading_periods
        ON sessions.school_year_type_reference.school_year = grading_periods.school_year
        AND sessions_grading_periods.grading_period_reference.grading_period_descriptor = grading_periods.grading_period_descriptor
        AND sessions_grading_periods.grading_period_reference.period_sequence = grading_periods.period_sequence
        AND sessions_grading_periods.grading_period_reference.school_id = grading_periods.school_reference.school_id
    WHERE sessions_grading_periods.grading_period_reference.grading_period_descriptor != ''

)

SELECT DISTINCT
    grading_period_key,
    session_key,
    school_key,
    school_year,
    grading_period_name,
    period_sequence,
    grading_period_begin_date,
    grading_period_end_date,
    total_instructional_days,
    IF(
        CURRENT_DATE BETWEEN grading_period_begin_date AND grading_period_end_date,
        TRUE,
        FALSE
    )                                                                 AS is_current_grading_period
FROM grades_grading_periods_unioned
