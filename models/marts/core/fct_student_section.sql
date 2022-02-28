
SELECT
    {{ dbt_utils.surrogate_key([
        'ssa.section_reference.school_id',
        'ssa.section_reference.school_year'
    ]) }}                                               AS school_key,
    {{ dbt_utils.surrogate_key([
        'course_offerings.session_reference.school_id',
        'course_offerings.session_reference.school_year',
        'course_offerings.session_reference.session_name'
    ]) }}                                               AS session_key,
    {{ dbt_utils.surrogate_key([
        'ssa.section_reference.school_id',
        'ssa.section_reference.school_year',
        'ssa.section_reference.session_name',
        'ssa.section_reference.local_course_code',
        'ssa.section_reference.section_identifier'
    ]) }}                                               AS section_key,
    {{ dbt_utils.surrogate_key([
        'ssa.section_reference.school_id',
        'ssa.section_reference.school_year',
        'ssa.section_reference.session_name',
        'ssa.section_reference.local_course_code',
        'ssa.section_reference.section_identifier'
    ]) }}                                               AS staff_group_key,
    {{ dbt_utils.surrogate_key([
        'ssa.student_reference.student_unique_id',
        'ssa.section_reference.school_year'
     ]) }}                                              AS student_key,
    section_reference.school_year                       AS school_year,
    ssa.homeroom_indicator                              AS homeroom_indicator,
    ssa.begin_date                                      AS start_date,
    ssa.end_date                                        AS end_date,
    IF(
        ssa.begin_date IS NULL
        OR (
            CURRENT_DATE >= ssa.begin_date
            AND CURRENT_DATE < ssa.end_date
        ),
        1, 0)                                           AS is_actively_enrolled_in_section
FROM {{ ref('stg_edfi_student_section_associations') }} ssa
LEFT JOIN {{ ref('stg_edfi_course_offerings') }} course_offerings
    ON ssa.school_year = course_offerings.school_year
    AND course_offerings.local_course_code = ssa.section_reference.local_course_code
    AND course_offerings.school_reference.school_id = ssa.section_reference.school_id
    AND course_offerings.session_reference.school_year = ssa.section_reference.school_year
    AND course_offerings.session_reference.session_name = ssa.section_reference.session_name
