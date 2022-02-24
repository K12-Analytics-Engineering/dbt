
SELECT
    {{ dbt_utils.surrogate_key([
            'ssa.student_reference.student_unique_id',
            'ssa.school_year_type_reference.school_year'
    ]) }}                                                           AS student_key,
    {{ dbt_utils.surrogate_key([
        'schools.local_education_agency_id',
        'ssa.school_year_type_reference.school_year'
    ]) }}                                                           AS local_education_agency_key,
    {{ dbt_utils.surrogate_key([
        'ssa.school_reference.school_id',
        'ssa.school_year_type_reference.school_year'
    ]) }}                                                           AS school_key,
    ssa.school_year_type_reference.school_year                      AS school_year,
    {{ convert_grade_level_to_short_label('ssa.entry_grade_level_descriptor') }}     AS grade_level,
    {{ convert_grade_level_to_id('ssa.entry_grade_level_descriptor') }}              AS grade_level_id,
    ssa.entry_date                                                  AS enrollment_date,
    ssa.entry_type_descriptor                                       AS enrollment_type,
    ssa.exit_withdraw_date                                          AS exit_date,
    ssa.exit_withdraw_type_descriptor                               AS exit_type,
    ssa.primary_school                                              AS is_primary_school,
    COUNT(calendar_dates.date)                                      AS count_days_enrolled,
    IF(
        ssa.exit_withdraw_date IS NULL
        OR (
            CURRENT_DATE >= ssa.entry_date
            AND CURRENT_DATE < ssa.exit_withdraw_date
        ),
        1, 0)                                                       AS is_actively_enrolled_in_school
FROM {{ ref('stg_edfi_student_school_associations') }} ssa
LEFT JOIN {{ ref('stg_edfi_schools') }} schools
    ON ssa.school_reference.school_id = schools.school_id
    AND ssa.school_year_type_reference.school_year = schools.school_year
LEFT JOIN {{ ref('stg_edfi_calendar_dates') }} calendar_dates
    ON ssa.school_year = calendar_dates.school_year
    AND ssa.school_reference.school_id = calendar_dates.calendar_reference.school_id
    AND ssa.entry_date <= calendar_dates.date
    AND (
        ssa.exit_withdraw_date IS NULL
        OR ssa.exit_withdraw_date > calendar_dates.date
    )
CROSS JOIN UNNEST(calendar_dates.calendar_events) AS calendar_events
WHERE
    calendar_dates.date < CURRENT_DATE
    AND calendar_events.calendar_event_descriptor = 'Instructional day'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
