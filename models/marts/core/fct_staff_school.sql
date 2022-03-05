
SELECT
    {{ dbt_utils.surrogate_key([
            'seoa.staff_reference.staff_unique_id',
            'seoa.school_year'
    ]) }}                                               AS staff_key,
    {{ dbt_utils.surrogate_key([
        'seoa.education_organization_reference.education_organization_id',
        'seoa.school_year'
    ]) }}                                               AS school_key,
    seoa.school_year                                    AS school_year,
    seoa.staff_classification_descriptor                AS staff_classification,
    ssa.academic_subjects                               AS academic_subjects,
    ssa.grade_levels                                    AS grade_levels,
    seoa.begin_date                                     AS begin_date,
    seoa.end_date                                       AS end_date,
    IF(CURRENT_DATE BETWEEN seoa.begin_date AND seoa.end_date, 1, 0) AS is_actively_assigned_to_school
FROM {{ ref('stg_edfi_staff_education_organization_assignment_associations') }} seoa
LEFT JOIN {{ ref('stg_edfi_staff_school_associations') }} ssa
    ON seoa.school_year = ssa.school_year
    AND seoa.staff_reference.staff_unique_id = ssa.staff_reference.staff_unique_id
    AND seoa.education_organization_reference.education_organization_id = ssa.school_reference.school_id
WHERE seoa.education_organization_reference.education_organization_id IN (

    SELECT school_id FROM {{ ref('stg_edfi_schools') }}

)
