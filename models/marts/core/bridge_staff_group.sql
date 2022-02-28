
SELECT
    {{ dbt_utils.surrogate_key([
        'section_reference.school_id',
        'section_reference.school_year',
        'section_reference.session_name',
        'section_reference.local_course_code',
        'section_reference.section_identifier'
    ]) }}                                            AS staff_group_key,
    {{ dbt_utils.surrogate_key([
        'staff_reference.staff_unique_id',
        'section_reference.school_year'
    ]) }}                                            AS staff_key,
    classroom_position_descriptor                    AS classroom_position,
    highly_qualified_teacher                         AS highly_qualified_teacher,
    percentage_contribution                          AS percentage_contribution
FROM {{ ref('stg_edfi_staff_section_associations') }}
WHERE CURRENT_DATE BETWEEN begin_date AND end_date
