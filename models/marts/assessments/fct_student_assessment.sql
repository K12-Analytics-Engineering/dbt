
-- student assessment score results
SELECT DISTINCT
    {{ dbt_utils.surrogate_key([
        'student_assessments.assessment_reference.assessment_identifier',
        'student_assessments.assessment_reference.namespace'
    ]) }}                                                                           AS assessment_key,
    ""                                                                              AS objective_assessment_key,
    {{ dbt_utils.surrogate_key([
        'student_assessments.student_reference.student_unique_id',
        'student_assessments.school_year'
    ]) }}                                                                           AS student_key,
    {{ dbt_utils.surrogate_key([
        'student_school_associations.school_reference.school_id',
        'student_assessments.school_year'
    ]) }}                                                                           AS school_key,
    student_assessments.school_year                                                 AS school_year,
    student_assessments.student_assessment_identifier                               AS student_assessment_identifier,
    student_assessments.administration_date                                         AS administration_date,
    student_assessments.when_assessed_grade_level_descriptor                        AS assessed_grade_level,
    score_results.assessment_reporting_method_descriptor                            AS reporting_method,
    score_results.result_datatype_type_descriptor                                   AS student_result_data_type,
    score_results.result                                                            AS student_result
FROM {{ ref('stg_edfi_student_assessments') }} student_assessments
LEFT JOIN UNNEST(student_assessments.score_results) AS score_results
LEFT JOIN UNNEST(student_assessments.performance_levels) AS performance_levels
LEFT JOIN {{ ref('stg_edfi_student_school_associations') }} student_school_associations
    ON student_assessments.school_year = student_school_associations.school_year
    AND student_assessments.student_reference.student_unique_id = student_school_associations.student_reference.student_unique_id
    AND student_assessments.administration_date >= student_school_associations.entry_date
    AND (
        student_assessments.administration_date <= student_school_associations.exit_withdraw_date
        OR student_school_associations.exit_withdraw_date IS NULL
    )


UNION ALL


--student assessment performance levels
SELECT DISTINCT
    {{ dbt_utils.surrogate_key([
        'student_assessments.assessment_reference.assessment_identifier',
        'student_assessments.assessment_reference.namespace'
    ]) }}                                                                           AS assessment_key,
    ""                                                                              AS objective_assessment_key,
    {{ dbt_utils.surrogate_key([
        'student_assessments.student_reference.student_unique_id',
        'student_assessments.school_year'
    ]) }}                                                                           AS student_key,
    {{ dbt_utils.surrogate_key([
        'student_school_associations.school_reference.school_id',
        'student_assessments.school_year'
    ]) }}                                                                           AS school_key,
    student_assessments.school_year                                                 AS school_year,
    student_assessments.student_assessment_identifier                               AS student_assessment_identifier,
    student_assessments.administration_date                                         AS administration_date,
    student_assessments.when_assessed_grade_level_descriptor                        AS assessed_grade_level,
    performance_levels.assessment_reporting_method_descriptor                       AS reporting_method,
    'Performance Level'                                                             AS student_result_data_type,
    performance_levels.performance_level_descriptor                                 AS student_result
FROM {{ ref('stg_edfi_student_assessments') }} student_assessments
LEFT JOIN UNNEST(student_assessments.performance_levels) AS performance_levels
LEFT JOIN {{ ref('stg_edfi_student_school_associations') }} student_school_associations
    ON student_assessments.school_year = student_school_associations.school_year
    AND student_assessments.student_reference.student_unique_id = student_school_associations.student_reference.student_unique_id
    AND student_assessments.administration_date >= student_school_associations.entry_date
    AND (
        student_assessments.administration_date <= student_school_associations.exit_withdraw_date
        OR student_school_associations.exit_withdraw_date IS NULL
    )


UNION ALL


-- student objective assessment score results
SELECT DISTINCT
    {{ dbt_utils.surrogate_key([
        'student_assessments.assessment_reference.assessment_identifier',
        'student_assessments.assessment_reference.namespace'
    ]) }}                                                                                AS assessment_key,
    {{ dbt_utils.surrogate_key([
        'student_assessments.assessment_reference.assessment_identifier',
        'student_assessments.assessment_reference.namespace',
        'student_objective_assessments.objective_assessment_reference.identification_code'
    ]) }}                                                                                AS objective_assessment_key,
    {{ dbt_utils.surrogate_key([
        'student_assessments.student_reference.student_unique_id',
        'student_assessments.school_year'
    ]) }}                                                                                AS student_key,
    {{ dbt_utils.surrogate_key([
        'student_school_associations.school_reference.school_id',
        'student_assessments.school_year'
    ]) }}                                                                                AS school_key,
    student_assessments.school_year                                                      AS school_year,
    student_assessments.student_assessment_identifier                                    AS student_assessment_identifier,
    student_assessments.administration_date                                              AS administration_date,
    student_assessments.when_assessed_grade_level_descriptor                             AS assessed_grade_level,
    student_objective_assessments_score_results.assessment_reporting_method_descriptor   AS reporting_method,
    student_objective_assessments_score_results.result_datatype_type_descriptor          AS student_result_data_type,
    student_objective_assessments_score_results.result                                   AS student_result
FROM {{ ref('stg_edfi_student_assessments') }} student_assessments
LEFT JOIN UNNEST(student_assessments.student_objective_assessments) AS student_objective_assessments
LEFT JOIN UNNEST(student_objective_assessments.score_results) AS student_objective_assessments_score_results
LEFT JOIN {{ ref('stg_edfi_student_school_associations') }} student_school_associations
    ON student_assessments.school_year = student_school_associations.school_year
    AND student_assessments.student_reference.student_unique_id = student_school_associations.student_reference.student_unique_id
    AND student_assessments.administration_date >= student_school_associations.entry_date
    AND (
        student_assessments.administration_date <= student_school_associations.exit_withdraw_date
        OR student_school_associations.exit_withdraw_date IS NULL
    )


UNION ALL


-- student objective assessment performance levels
SELECT DISTINCT
    {{ dbt_utils.surrogate_key([
        'student_assessments.assessment_reference.assessment_identifier',
        'student_assessments.assessment_reference.namespace'
    ]) }}                                                                                     AS assessment_key,
    {{ dbt_utils.surrogate_key([
        'student_assessments.assessment_reference.assessment_identifier',
        'student_assessments.assessment_reference.namespace',
        'student_objective_assessments.objective_assessment_reference.identification_code'
    ]) }}                                                                                     AS objective_assessment_key,
    {{ dbt_utils.surrogate_key([
        'student_assessments.student_reference.student_unique_id',
        'student_assessments.school_year'
    ]) }}                                                                                     AS student_key,
    {{ dbt_utils.surrogate_key([
        'student_school_associations.school_reference.school_id',
        'student_assessments.school_year'
    ]) }}                                                                                     AS school_key,
    student_assessments.school_year                                                           AS school_year,
    student_assessments.student_assessment_identifier                                         AS student_assessment_identifier,
    student_assessments.administration_date                                                   AS administration_date,
    student_assessments.when_assessed_grade_level_descriptor                                  AS assessed_grade_level,
    student_objective_assessments_performance_levels.assessment_reporting_method_descriptor   AS reporting_method,
    'Performance Level'                                                                       AS student_result_data_type,
    student_objective_assessments_performance_levels.performance_level_descriptor             AS student_result
FROM {{ ref('stg_edfi_student_assessments') }} student_assessments
LEFT JOIN UNNEST(student_assessments.student_objective_assessments) AS student_objective_assessments
LEFT JOIN UNNEST(student_objective_assessments.performance_levels) AS student_objective_assessments_performance_levels
LEFT JOIN {{ ref('stg_edfi_student_school_associations') }} student_school_associations
    ON student_assessments.school_year = student_school_associations.school_year
    AND student_assessments.student_reference.student_unique_id = student_school_associations.student_reference.student_unique_id
    AND student_assessments.administration_date >= student_school_associations.entry_date
    AND (
        student_assessments.administration_date <= student_school_associations.exit_withdraw_date
        OR student_school_associations.exit_withdraw_date IS NULL
    )
