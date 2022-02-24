
SELECT DISTINCT
    {{ dbt_utils.surrogate_key([
        'seoa.staff_reference.staff_unique_id' 
    ]) }}                                                   AS user_key,
    {{ dbt_utils.surrogate_key([
        'ssa.student_reference.student_unique_id'
    ]) }}                                                   AS student_key
FROM  {{ ref('stg_edfi_staff_education_organization_assignment_associations') }} seoa
LEFT JOIN {{ ref('stg_edfi_schools') }} schools
    ON seoa.school_year = schools.school_year
    AND schools.local_education_agency_id = seoa.education_organization_reference.education_organization_id
LEFT JOIN {{ ref('stg_edfi_student_school_associations') }} ssa
    ON seoa.school_year = ssa.school_year
    AND ssa.school_reference.school_id = schools.school_id
WHERE
    seoa.staff_classification_descriptor = 'Superintendent'
    AND (seoa.end_date IS NOT NULL OR seoa.end_date >= CURRENT_DATE)
    AND (ssa.exit_withdraw_date IS NOT NULL OR ssa.exit_withdraw_date >= CURRENT_DATE)

UNION ALL

SELECT
    {{ dbt_utils.surrogate_key([
        'seoa.staff_reference.staff_unique_id'
    ]) }}                                                   AS user_key,
    {{ dbt_utils.surrogate_key([
        'ssa.student_reference.student_unique_id'
    ]) }}                                                   AS student_key
FROM  {{ ref('stg_edfi_staff_education_organization_assignment_associations') }} seoa
LEFT JOIN{{ ref('stg_edfi_student_school_associations') }} ssa
    ON seoa.school_year = ssa.school_year
    AND ssa.school_reference.school_id = seoa.education_organization_reference.education_organization_id
WHERE
    seoa.staff_classification_descriptor = 'Principal'
    AND (seoa.end_date IS NOT NULL OR seoa.end_date >= CURRENT_DATE)
    AND (ssa.exit_withdraw_date IS NOT NULL OR ssa.exit_withdraw_date >= CURRENT_DATE)

UNION ALL

SELECT
    {{ dbt_utils.surrogate_key([
        'seoa.staff_reference.staff_unique_id' ])
    }}                                                      AS user_key,
    {{ dbt_utils.surrogate_key([
        'student_section_associations.student_reference.student_unique_id'
    ]) }}                                                   AS student_key
FROM  {{ ref('stg_edfi_staff_education_organization_assignment_associations') }} seoa
LEFT JOIN {{ ref('stg_edfi_staff_section_associations') }} staff_section_associations
    ON seoa.school_year = staff_section_associations.school_year
    AND staff_section_associations.staff_reference.staff_unique_id = seoa.staff_reference.staff_unique_id
    AND staff_section_associations.section_reference.school_id = seoa.education_organization_reference.education_organization_id
LEFT JOIN {{ ref('stg_edfi_student_section_associations') }} student_section_associations
    ON seoa.school_year = student_section_associations.school_year
    AND student_section_associations.section_reference.local_course_code = staff_section_associations.section_reference.local_course_code
    AND student_section_associations.section_reference.school_id = staff_section_associations.section_reference.school_id
    AND student_section_associations.section_reference.school_year = staff_section_associations.section_reference.school_year
    AND student_section_associations.section_reference.section_identifier = staff_section_associations.section_reference.section_identifier
    AND student_section_associations.section_reference.session_name = staff_section_associations.section_reference.session_name
WHERE
    seoa.staff_classification_descriptor = 'Teacher'
    AND (student_section_associations.end_date IS NOT NULL OR student_section_associations.end_date >= CURRENT_DATE)

