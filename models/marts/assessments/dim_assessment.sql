
select
    {{ dbt_utils.surrogate_key([
        'assessments.assessment_identifier',
        'assessments.namespace',
    ]) }}                                               as assessment_key,
    {{ dbt_utils.surrogate_key([
        'education_organization_reference.education_organization_id',
        'assessments.school_year'
    ]) }}                                               as education_organization_key,
    assessments.school_year                             as school_year,
    assessments.assessment_identifier                   as assessment_identifier,
    assessments.assessment_family                       as assessment_family,
    assessments.namespace                               as namespace,
    assessments.assessment_title                        as title,
    ifnull(assessments.assessment_version, 0)           as version,
    assessments.assessment_category_descriptor	        as category,
    assessment_form                                     as form,
    if(adaptive_assessment is true, 'Yes', 'No')        as adaptive_assessment,
    NULL                                                as objective_assessment_identification_code,
    NULL                                                as objective_assessment_academic_subject,
    NULL                                                as objective_assessment_description
from {{ ref('stg_edfi_assessments') }} assessments


union all


select
    {{ dbt_utils.surrogate_key([
        'assessments.assessment_identifier',
        'assessments.namespace',
        'objective_assessments.identification_code'
    ]) }}                                               as assessment_key,
    {{ dbt_utils.surrogate_key([
        'education_organization_reference.education_organization_id',
        'assessments.school_year'
    ]) }}                                               as education_organization_key,
    assessments.school_year                             as school_year,
    assessments.assessment_identifier                   as assessment_identifier,
    assessments.assessment_family                       as assessment_family,
    assessments.namespace                               as namespace,
    assessments.assessment_title                        as title,
    ifnull(assessments.assessment_version, 0)           as version,
    assessments.assessment_category_descriptor	        as category,
    assessment_form                                     as form,
    if(adaptive_assessment is true, 'Yes', 'No')        as adaptive_assessment,
    objective_assessments.identification_code           as objective_assessment_identification_code,
    objective_assessments.academic_subject_descriptor   as objective_assessment_academic_subject,
    objective_assessments.description                   as objective_assessment_description
from {{ ref('stg_edfi_assessments') }} assessments
left join {{ ref('stg_edfi_objective_assessments') }} objective_assessments
    on assessments.assessment_identifier = objective_assessments.assessment_reference.assessment_identifier
    and assessments.namespace = objective_assessments.assessment_reference.namespace
    and assessments.school_year = objective_assessments.school_year
