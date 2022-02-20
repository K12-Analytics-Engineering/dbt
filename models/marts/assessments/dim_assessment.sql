
SELECT
    {{ dbt_utils.surrogate_key([
        'assessments.assessment_identifier',
        'assessments.namespace'
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
    IF(adaptive_assessment IS TRUE, 'Yes', 'No')        AS adaptive_assessment
FROM {{ ref('stg_edfi_assessments') }} assessments
