
with students as (

    select
        {{ dbt_utils.surrogate_key([
                'students.student_unique_id',
                'students.school_year'
        ]) }}                                                           as student_key,
        students.school_year                                            as school_year,
        students.student_unique_id                                      as student_unique_id,
        students.first_name                                             as student_first_name,
        students.middle_name                                            as student_middle_name,
        students.last_surname                                           as student_last_surname,
        concat(
            students.last_surname, ', ',
            students.first_name, ' ',
            coalesce(left(students.middle_name, 1), '')
        )                                                               as student_display_name,
        seoa.electronic_mail[safe_offset(0)].address                    as student_email,
        coalesce(
            seoa.limited_english_proficiency_descriptor,
            'Not applicable'
        )                                                               as limited_english_proficiency,
        if(
            seoa.limited_english_proficiency_descriptor = "Limited",
            "Yes",
            "No"
        )                                                               as is_english_language_learner,
        if(
            edfi_programs.program_name is not null,
            "Yes",
            "No"
        )                                                               as in_special_education_program,
        if(seoa.hispanic_latino_ethnicity is true, 'Yes', 'No')         as is_hispanic,
        case
            when seoa.hispanic_latino_ethnicity is true then 'Hispanic or Latino'
            when array_length(seoa.races) > 1 then 'Two or more races'
            when array_length(seoa.races) = 0 then 'Unknown'
            else seoa.races[offset(0)].race_descriptor
        end                                                             as race_and_ethnicity_roll_up,
        seoa.sex_descriptor                                             as gender,
        students.birth_date                                             as birth_date
    from {{ ref('stg_edfi_students') }} students
    left join {{ ref('stg_edfi_student_education_organization_associations') }} seoa 
        on students.student_unique_id = seoa.student_reference.student_unique_id
        and students.school_year = seoa.school_year
    left join {{ ref('stg_edfi_student_special_education_program_associations') }} edfi_student_sped_associations
        on students.school_year = edfi_student_sped_associations.school_year
        and seoa.education_organization_reference.education_organization_id = edfi_student_sped_associations.program_reference.education_organization_id
        and students.student_unique_id = edfi_student_sped_associations.student_reference.student_unique_id
    left join {{ ref('stg_edfi_programs') }} edfi_programs
        on edfi_student_sped_associations.school_year = edfi_programs.school_year
        and edfi_student_sped_associations.program_reference.program_type_descriptor = edfi_programs.program_type_descriptor
        and edfi_student_sped_associations.program_reference.education_organization_id = edfi_programs.education_organization_reference.education_organization_id
        and edfi_programs.program_name = "Special Education"

)


select
    student_key,
    school_year,
    student_unique_id,
    student_first_name,
    student_middle_name,
    student_last_surname,
    student_display_name,
    student_email,
    ifnull(race_and_ethnicity_roll_up, 'Unknown') as race_and_ethnicity_roll_up,
    gender,
    birth_date,
    limited_english_proficiency         as limited_english_proficiency,
    max(is_english_language_learner)    as is_english_language_learner,
    max(in_special_education_program)   as in_special_education_program,
    max(is_hispanic)                    as is_hispanic
from students
{{ dbt_utils.group_by(n=12) }}
