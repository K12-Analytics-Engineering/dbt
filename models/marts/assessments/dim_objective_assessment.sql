
SELECT
    {{ dbt_utils.surrogate_key([
        'assessments.assessment_identifier',
        'assessments.namespace',
        'objective_assessments.identification_code'
    ]) }}                                               AS objective_assessment_key,
    {{ dbt_utils.surrogate_key([
        'assessments.assessment_identifier',
        'assessments.namespace',
    ]) }}                                               AS assessment_key,
    objective_assessments.school_year                   AS school_year,
    objective_assessments.identification_code           AS identification_code,
    objective_assessments.academic_subject_descriptor   AS academic_subject,
    objective_assessments.description                   AS description
FROM {{ ref('stg_edfi_objective_assessments') }} objective_assessments
LEFT JOIN {{ ref('stg_edfi_assessments') }} assessments
    ON objective_assessments.assessment_reference.assessment_identifier = assessments.assessment_identifier
    AND objective_assessments.assessment_reference.namespace = assessments.namespace
    AND objective_assessments.school_year = assessments.school_year
