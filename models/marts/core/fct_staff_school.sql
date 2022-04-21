
select
    {{ dbt_utils.surrogate_key([
            'seoa.staff_reference.staff_unique_id',
            'seoa.school_year'
    ]) }}                                               as staff_key,
    {{ dbt_utils.surrogate_key([
        'seoa.education_organization_reference.education_organization_id',
        'seoa.school_year'
    ]) }}                                               as school_key,
    seoa.school_year                                    as school_year,
    seoa.staff_classification_descriptor                as staff_classification,
    ssa.academic_subjects                               as academic_subjects,
    ssa.grade_levels                                    as grade_levels,
    seoa.begin_date                                     as begin_date,
    seoa.end_date                                       as end_date,
    if(current_date between seoa.begin_date and seoa.end_date, 1, 0) as is_actively_assigned_to_school
from {{ ref('stg_edfi_staff_education_organization_assignment_associations') }} seoa
left join {{ ref('stg_edfi_staff_school_associations') }} ssa
    on seoa.school_year = ssa.school_year
    and seoa.staff_reference.staff_unique_id = ssa.staff_reference.staff_unique_id
    and seoa.education_organization_reference.education_organization_id = ssa.school_reference.school_id
where seoa.education_organization_reference.education_organization_id in (

    select school_id from {{ ref('stg_edfi_schools') }}

)
