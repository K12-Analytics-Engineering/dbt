
WITH staff_to_scope_map AS (
    SELECT
        seoa.school_year,
        seoa.staff_reference.staff_unique_id,
        seoa.staff_classification_descriptor AS user_scope,
        seoa.education_organization_reference.education_organization_id
    FROM  {{ ref('stg_edfi_staff_education_organization_assignment_associations') }} seoa
    WHERE
        seoa.staff_classification_descriptor IN ('Superintendent', 'Principal', 'Teacher')
        AND (seoa.end_date IS NOT NULL OR seoa.end_date >= CURRENT_DATE)
)

SELECT DISTINCT
    {{ dbt_utils.surrogate_key([
        'staff_to_scope_map.staff_unique_id'
     ]) }}                                          AS user_key,
    staff_to_scope_map.user_scope                   AS user_scope,
    'ALL'                                           AS student_permission,
    CASE staff_to_scope_map.user_scope
        WHEN 'Superintendent' THEN 'ALL'
        WHEN 'Principal' THEN 'ALL'
        ELSE edfi_sections.section_identifier
    END                                             AS section_permission,
    CASE staff_to_scope_map.user_scope
        WHEN 'Superintendent' THEN 'ALL'
		WHEN 'Principal'      THEN 'ALL'
        ELSE CONCAT(
            edfi_sections.course_offering_reference.school_id, '-',
            edfi_sections.course_offering_reference.local_course_code, '-',
            edfi_sections.course_offering_reference.school_year, '-',
            edfi_sections.section_identifier, '-',
            edfi_sections.course_offering_reference.session_name
        )
    END                                             AS section_key_permission,
    CASE staff_to_scope_map.user_scope
        WHEN 'Superintendent' THEN 'ALL'
        ELSE staff_to_scope_map.education_organization_id
    END                                             AS school_permission,
    IF(staff_to_scope_map.user_scope = 'Superintendent', staff_to_scope_map.education_organization_id, NULL) AS district_id
FROM staff_to_scope_map
LEFT JOIN {{ ref('stg_edfi_staff_section_associations') }} staff_section_associations
    ON staff_to_scope_map.school_year = staff_section_associations.school_year
    AND staff_section_associations.staff_reference.staff_unique_id = staff_to_scope_map.staff_unique_id
    AND staff_section_associations.section_reference.school_id = staff_to_scope_map.education_organization_id
LEFT JOIN {{ ref('stg_edfi_sections') }} edfi_sections
    ON staff_to_scope_map.school_year = edfi_sections.school_year
    AND edfi_sections.course_offering_reference.local_course_code = staff_section_associations.section_reference.local_course_code
    AND edfi_sections.course_offering_reference.school_id = staff_section_associations.section_reference.school_id
    AND edfi_sections.course_offering_reference.school_year = staff_section_associations.section_reference.school_year
    AND edfi_sections.section_identifier = staff_section_associations.section_reference.section_identifier
    AND edfi_sections.course_offering_reference.session_name = staff_section_associations.section_reference.session_name
WHERE
    staff_to_scope_map.user_scope IN ('Superintendent', 'Principal', 'Teacher')
    OR staff_section_associations.staff_reference.staff_unique_id IS NOT NULL
