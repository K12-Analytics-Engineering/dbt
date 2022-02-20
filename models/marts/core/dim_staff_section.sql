

SELECT
    {{ dbt_utils.surrogate_key([
        'ssa.section_reference.school_id',
        'ssa.section_reference.school_year',
        'ssa.section_reference.session_name',
        'ssa.section_reference.local_course_code',
        'ssa.section_reference.section_identifier',
        'ssa.staff_reference.staff_unique_id',
        'ssa.begin_date'
    ]) }}                                               AS staff_section_key,
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
        'ssa.staff_reference.staff_unique_id',
        'ssa.section_reference.school_year'
     ]) }}                                              AS staff_key,
    section_reference.school_year                       AS school_year,
    section_reference.local_course_code                 AS local_course_code,
    courses.academic_subject_descriptor                 AS academic_subject,
    courses.course_title                                AS course_title,
    ssa.classroom_position_descriptor                   AS classroom_position,
    ssa.begin_date                                      AS staff_section_begin_date,
    ssa.end_date                                        AS staff_section_end_date
FROM {{ ref('stg_edfi_staff_section_associations') }} ssa
LEFT JOIN {{ ref('stg_edfi_course_offerings') }} course_offerings
    ON ssa.school_year = course_offerings.school_year
    AND course_offerings.local_course_code = ssa.section_reference.local_course_code
    AND course_offerings.school_reference.school_id = ssa.section_reference.school_id
    AND course_offerings.session_reference.school_year = ssa.section_reference.school_year
    AND course_offerings.session_reference.session_name = ssa.section_reference.session_name
LEFT JOIN {{ ref('stg_edfi_courses') }} courses
    ON course_offerings.school_year = courses.school_year
    AND courses.course_code = course_offerings.course_reference.course_code
    AND courses.education_organization_reference.education_organization_id = course_offerings.course_reference.education_organization_id
