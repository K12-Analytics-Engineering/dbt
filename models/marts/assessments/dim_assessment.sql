
SELECT
    {{ dbt_utils.surrogate_key([
        'assessments.assessment_identifier',
        'assessments.namespace',
    ]) }}                                               AS assessment_key,
    {{ dbt_utils.surrogate_key([
        'education_organization_reference.education_organization_id',
        'assessments.school_year'
    ]) }}                                               AS education_organization_key,
    assessments.school_year                             AS school_year,
    assessments.assessment_identifier                   AS assessment_identifier,
    assessments.assessment_family                       AS assessment_family,
    assessments.namespace                               AS namespace,
    assessments.assessment_title                        AS title,
    IFNULL(assessments.assessment_version, 0)           AS version,
    assessments.assessment_category_descriptor	        AS category,
    assessment_form                                     AS form,
    IF(adaptive_assessment IS TRUE, 'Yes', 'No')        AS adaptive_assessment,
    NULL                                                AS objective_assessment_identification_code,
    NULL                                                AS objective_assessment_academic_subject,
    NULL                                                AS objective_assessment_description
FROM {{ ref('stg_edfi_assessments') }} assessments


UNION ALL


SELECT
    {{ dbt_utils.surrogate_key([
        'assessments.assessment_identifier',
        'assessments.namespace',
        'objective_assessments.identification_code'
    ]) }}                                               AS assessment_key,
    {{ dbt_utils.surrogate_key([
        'education_organization_reference.education_organization_id',
        'assessments.school_year'
    ]) }}                                               AS education_organization_key,
    assessments.school_year                             AS school_year,
    assessments.assessment_identifier                   AS assessment_identifier,
    assessments.assessment_family                       AS assessment_family,
    assessments.namespace                               AS namespace,
    assessments.assessment_title                        AS title,
    IFNULL(assessments.assessment_version, 0)           AS version,
    assessments.assessment_category_descriptor	        AS category,
    assessment_form                                     AS form,
    IF(adaptive_assessment IS TRUE, 'Yes', 'No')        AS adaptive_assessment,
    objective_assessments.identification_code           AS objective_assessment_identification_code,
    objective_assessments.academic_subject_descriptor   AS objective_assessment_academic_subject,
    objective_assessments.description                   AS objective_assessment_description
FROM {{ ref('stg_edfi_assessments') }} assessments
LEFT JOIN {{ ref('stg_edfi_objective_assessments') }} objective_assessments
    ON assessments.assessment_identifier = objective_assessments.assessment_reference.assessment_identifier
    AND assessments.namespace = objective_assessments.assessment_reference.namespace
    AND assessments.school_year = objective_assessments.school_year
