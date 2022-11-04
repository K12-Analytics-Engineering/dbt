
with assessments as (

    select
        fct_student_assessment.school_year,
        fct_student_assessment.student_assessment_identifier,
        ARRAY_AGG(
            struct(
                fct_student_assessment.reporting_method                    as reporting_method,
                fct_student_assessment.student_result                      as student_result
            )
        ) as assessment_student_score
    from {{ ref('fct_student_assessment') }} fct_student_assessment
    left join {{ ref('dim_assessment') }} dim_assessment
        on fct_student_assessment.assessment_key = dim_assessment.assessment_key
    where dim_assessment.objective_assessment_identification_code is null
    group by 1, 2

),

objective_assessments as (

    select
        fct_student_assessment.school_year,
        fct_student_assessment.student_assessment_identifier,
        ARRAY_AGG(
            struct(
                dim_assessment.objective_assessment_identification_code              as identification_code,
                dim_assessment.objective_assessment_description                      as description,
                fct_student_assessment.reporting_method                                        as reporting_method,
                fct_student_assessment.student_result                                          as student_result
            )
        ) as objective_assessment_student_score
    from {{ ref('fct_student_assessment') }} fct_student_assessment
    left join {{ ref('dim_assessment') }} dim_assessment
        on fct_student_assessment.assessment_key = dim_assessment.assessment_key
    where dim_assessment.objective_assessment_identification_code is not null
    group by 1, 2

)

select
    fct_student_assessment.school_year                          as school_year,
    dim_school.school_name                                      as school_name,
    dim_student.student_unique_id                               as student_unique_id,
    dim_student.student_last_surname                            as student_last_surname,
    dim_student.student_first_name                              as student_first_name,
    dim_student.student_display_name                            as student_display_name,
    dim_student.is_actively_enrolled_in_school                  as is_actively_enrolled_in_school,
    dim_student.grade_level                                     as grade_level,
    dim_student.grade_level_id                                  as grade_level_id,
    dim_student.gender                                          as gender,
    dim_student.limited_english_proficiency                     as limited_english_proficiency,
    dim_student.is_english_language_learner                     as is_english_language_learner,
    dim_student.in_special_education_program                    as in_special_education_program,
    dim_student.is_hispanic                                     as is_hispanic,
    dim_student.race_and_ethnicity_roll_up                      as race_and_ethnicity_roll_up,
    dim_assessment.assessment_identifier                        as assessment_identifier,
    dim_assessment.title                                        as title,
    dim_assessment.namespace                                    as namespace,
    fct_student_assessment.student_assessment_identifier        as student_assessment_identifier,
    objective_assessments.objective_assessment_student_score    as objective_assessment_student_score,
    assessments.assessment_student_score                        as assessment_student_score
from {{ ref('fct_student_assessment') }} fct_student_assessment
left join {{ ref('dim_assessment') }} dim_assessment
    on fct_student_assessment.assessment_key = dim_assessment.assessment_key
left join assessments
    on fct_student_assessment.student_assessment_identifier = assessments.student_assessment_identifier
left join objective_assessments
    on fct_student_assessment.student_assessment_identifier = objective_assessments.student_assessment_identifier
left join {{ ref('dim_student') }} dim_student
    on fct_student_assessment.student_key = dim_student.student_key
left join {{ ref('dim_school') }} dim_school
    on fct_student_assessment.school_key = dim_school.school_key
where 
    dim_assessment.objective_assessment_identification_code is null
    and dim_student.student_unique_id is not null
