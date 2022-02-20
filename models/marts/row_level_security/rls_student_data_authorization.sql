{{
  config(
    labels = {'analytics_middle_tier': 'yes'}
  )
}}


SELECT
    {{ dbt_utils.surrogate_key([
        'ssa.student_reference.student_unique_id'
    ]) }}                                           AS student_key,
    {{ dbt_utils.surrogate_key([
        'ssa.section_reference.school_id'
    ]) }}                                           AS school_key,
    {{ dbt_utils.surrogate_key([
        'sections.id'
     ]) }}                                          AS section_id,
    ssa.begin_date,
    ssa.end_date
FROM {{ ref('stg_edfi_student_section_associations') }} ssa
LEFT JOIN {{ ref('stg_edfi_sections') }} sections
    ON ssa.school_year = sections.school_year
    AND sections.course_offering_reference.local_course_code = ssa.section_reference.local_course_code
    AND sections.course_offering_reference.school_id = ssa.section_reference.school_id
    AND sections.course_offering_reference.school_year = ssa.section_reference.school_year
    AND sections.section_identifier = ssa.section_reference.section_identifier
    AND sections.course_offering_reference.session_name = ssa.section_reference.session_name
