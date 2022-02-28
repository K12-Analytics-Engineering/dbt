
SELECT
    {{ dbt_utils.surrogate_key([
        'staff_section_associations.section_reference.school_id',
        'staff_section_associations.section_reference.school_year',
        'staff_section_associations.section_reference.session_name',
        'staff_section_associations.section_reference.local_course_code',
        'staff_section_associations.section_reference.section_identifier'
    ]) }}                                                                       AS staff_group_key,
    {{ dbt_utils.surrogate_key([
        'staff_section_associations.staff_reference.staff_unique_id',
        'staff_section_associations.section_reference.school_year'
    ]) }}                                                                       AS staff_key,
    staff_section_associations.classroom_position_descriptor                    AS classroom_position,
    staff_section_associations.highly_qualified_teacher                         AS highly_qualified_teacher,
    staff_section_associations.percentage_contribution                          AS percentage_contribution
FROM {{ ref('stg_edfi_staff_section_associations') }} staff_section_associations
LEFT JOIN {{ ref('stg_edfi_sections') }} sections
    ON staff_section_associations.section_reference.local_course_code = sections.course_offering_reference.local_course_code
    AND staff_section_associations.section_reference.school_id = sections.course_offering_reference.school_id
    AND staff_section_associations.section_reference.school_year = sections.course_offering_reference.school_year
    AND staff_section_associations.section_reference.section_identifier = sections.section_identifier
    AND staff_section_associations.section_reference.session_name = sections.course_offering_reference.session_name
LEFT JOIN {{ ref('stg_edfi_course_offerings') }} course_offerings
    ON sections.course_offering_reference.local_course_code = course_offerings.local_course_code
    AND sections.course_offering_reference.school_id = course_offerings.school_reference.school_id
    AND sections.course_offering_reference.school_year = course_offerings.session_reference.school_year
    AND sections.course_offering_reference.session_name = course_offerings.session_reference.session_name
LEFT JOIN {{ ref('stg_edfi_sessions') }} sessions
    ON course_offerings.school_reference.school_id = sessions.school_reference.school_id
    AND course_offerings.session_reference.school_year = sessions.school_year_type_reference.school_year
    AND course_offerings.session_reference.session_name = sessions.session_name
WHERE
    CURRENT_DATE BETWEEN staff_section_associations.begin_date AND staff_section_associations.end_date
    OR staff_section_associations.end_date = sessions.end_date
