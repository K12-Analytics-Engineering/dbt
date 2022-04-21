
-- student assessment score results
select distinct
    {{ dbt_utils.surrogate_key([
        'student_assessments.assessment_reference.assessment_identifier',
        'student_assessments.assessment_reference.namespace'
    ]) }}                                                                           as assessment_key,
    {{ dbt_utils.surrogate_key([
        'student_assessments.student_reference.student_unique_id',
        'student_assessments.school_year'
    ]) }}                                                                           as student_key,
    {{ dbt_utils.surrogate_key([
        'student_school_associations.school_reference.school_id',
        'student_assessments.school_year'
    ]) }}                                                                           as school_key,
    student_assessments.school_year                                                 as school_year,
    student_assessments.student_assessment_identifier                               as student_assessment_identifier,
    student_assessments.administration_date                                         as administration_date,
    student_assessments.when_assessed_grade_level_descriptor                        as assessed_grade_level,
    score_results.assessment_reporting_method_descriptor                            as reporting_method,
    score_results.result_datatype_type_descriptor                                   as student_result_data_type,
    score_results.result                                                            as student_result
from {{ ref('stg_edfi_student_assessments') }} student_assessments
left join unnest(student_assessments.score_results) as score_results
left join unnest(student_assessments.performance_levels) as performance_levels
left join {{ ref('stg_edfi_student_school_associations') }} student_school_associations
    on student_assessments.school_year = student_school_associations.school_year
    and student_assessments.student_reference.student_unique_id = student_school_associations.student_reference.student_unique_id
    and student_assessments.administration_date >= student_school_associations.entry_date
    and (
        student_assessments.administration_date <= student_school_associations.exit_withdraw_date
        or student_school_associations.exit_withdraw_date is null
    )


union all


--student assessment performance levels
select distinct
    {{ dbt_utils.surrogate_key([
        'student_assessments.assessment_reference.assessment_identifier',
        'student_assessments.assessment_reference.namespace'
    ]) }}                                                                           as assessment_key,
    {{ dbt_utils.surrogate_key([
        'student_assessments.student_reference.student_unique_id',
        'student_assessments.school_year'
    ]) }}                                                                           as student_key,
    {{ dbt_utils.surrogate_key([
        'student_school_associations.school_reference.school_id',
        'student_assessments.school_year'
    ]) }}                                                                           as school_key,
    student_assessments.school_year                                                 as school_year,
    student_assessments.student_assessment_identifier                               as student_assessment_identifier,
    student_assessments.administration_date                                         as administration_date,
    student_assessments.when_assessed_grade_level_descriptor                        as assessed_grade_level,
    performance_levels.assessment_reporting_method_descriptor                       as reporting_method,
    'Performance Level'                                                             as student_result_data_type,
    performance_levels.performance_level_descriptor                                 as student_result
from {{ ref('stg_edfi_student_assessments') }} student_assessments
left join unnest(student_assessments.performance_levels) as performance_levels
left join {{ ref('stg_edfi_student_school_associations') }} student_school_associations
    on student_assessments.school_year = student_school_associations.school_year
    and student_assessments.student_reference.student_unique_id = student_school_associations.student_reference.student_unique_id
    and student_assessments.administration_date >= student_school_associations.entry_date
    and (
        student_assessments.administration_date <= student_school_associations.exit_withdraw_date
        or student_school_associations.exit_withdraw_date is null
    )


union all


-- student objective assessment score results
select distinct
    {{ dbt_utils.surrogate_key([
        'student_assessments.assessment_reference.assessment_identifier',
        'student_assessments.assessment_reference.namespace',
        'student_objective_assessments.objective_assessment_reference.identification_code'
    ]) }}                                                                                as assessment_key,
    {{ dbt_utils.surrogate_key([
        'student_assessments.student_reference.student_unique_id',
        'student_assessments.school_year'
    ]) }}                                                                                as student_key,
    {{ dbt_utils.surrogate_key([
        'student_school_associations.school_reference.school_id',
        'student_assessments.school_year'
    ]) }}                                                                                as school_key,
    student_assessments.school_year                                                      as school_year,
    student_assessments.student_assessment_identifier                                    as student_assessment_identifier,
    student_assessments.administration_date                                              as administration_date,
    student_assessments.when_assessed_grade_level_descriptor                             as assessed_grade_level,
    student_objective_assessments_score_results.assessment_reporting_method_descriptor   as reporting_method,
    student_objective_assessments_score_results.result_datatype_type_descriptor          as student_result_data_type,
    student_objective_assessments_score_results.result                                   as student_result
from {{ ref('stg_edfi_student_assessments') }} student_assessments
left join unnest(student_assessments.student_objective_assessments) as student_objective_assessments
left join unnest(student_objective_assessments.score_results) as student_objective_assessments_score_results
left join {{ ref('stg_edfi_student_school_associations') }} student_school_associations
    on student_assessments.school_year = student_school_associations.school_year
    and student_assessments.student_reference.student_unique_id = student_school_associations.student_reference.student_unique_id
    and student_assessments.administration_date >= student_school_associations.entry_date
    and (
        student_assessments.administration_date <= student_school_associations.exit_withdraw_date
        or student_school_associations.exit_withdraw_date is null
    )


union all


-- student objective assessment performance levels
select distinct
    {{ dbt_utils.surrogate_key([
        'student_assessments.assessment_reference.assessment_identifier',
        'student_assessments.assessment_reference.namespace',
        'student_objective_assessments.objective_assessment_reference.identification_code'
    ]) }}                                                                                     as assessment_key,
    {{ dbt_utils.surrogate_key([
        'student_assessments.student_reference.student_unique_id',
        'student_assessments.school_year'
    ]) }}                                                                                     as student_key,
    {{ dbt_utils.surrogate_key([
        'student_school_associations.school_reference.school_id',
        'student_assessments.school_year'
    ]) }}                                                                                     as school_key,
    student_assessments.school_year                                                           as school_year,
    student_assessments.student_assessment_identifier                                         as student_assessment_identifier,
    student_assessments.administration_date                                                   as administration_date,
    student_assessments.when_assessed_grade_level_descriptor                                  as assessed_grade_level,
    student_objective_assessments_performance_levels.assessment_reporting_method_descriptor   as reporting_method,
    'Performance Level'                                                                       as student_result_data_type,
    student_objective_assessments_performance_levels.performance_level_descriptor             as student_result
from {{ ref('stg_edfi_student_assessments') }} student_assessments
left join unnest(student_assessments.student_objective_assessments) as student_objective_assessments
left join unnest(student_objective_assessments.performance_levels) as student_objective_assessments_performance_levels
left join {{ ref('stg_edfi_student_school_associations') }} student_school_associations
    on student_assessments.school_year = student_school_associations.school_year
    and student_assessments.student_reference.student_unique_id = student_school_associations.student_reference.student_unique_id
    and student_assessments.administration_date >= student_school_associations.entry_date
    and (
        student_assessments.administration_date <= student_school_associations.exit_withdraw_date
        or student_school_associations.exit_withdraw_date is null
    )
