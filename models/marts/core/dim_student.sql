
with school_year_end_dates as (

    select
        school_year_type_reference.school_year,
        school_reference.school_id,
        max(end_date) as school_year_end_date
    from {{ ref('stg_edfi_sessions') }}
    group by 1, 2


),

active_enrollments as (

    select distinct
        student_school_associations.student_reference.student_unique_id            as student_unique_id,
        student_school_associations.school_year_type_reference.school_year         as school_year,
        'Yes'                                                                      as is_actively_enrolled_in_school
    from {{ ref('stg_edfi_student_school_associations') }} student_school_associations
    left join school_year_end_dates
        on student_school_associations.school_year_type_reference.school_year = school_year_end_dates.school_year
        and student_school_associations.school_reference.school_id = school_year_end_dates.school_id
    where
        student_school_associations.exit_withdraw_date is null
        or (
            -- active enrollment for current year
            current_date >= student_school_associations.entry_date
            and current_date < student_school_associations.exit_withdraw_date
        )
        -- if student exited a previous year on the final day of the session
        -- replace school_year_end_dates.school_year_end_date with DATE 'YYYY-MM-DD' if max session end date does
        -- not represent school year end date
        or student_school_associations.exit_withdraw_date >= school_year_end_dates.school_year_end_date

),

student_grade_level_ranked as (

    select
        school_year_type_reference.school_year,
        student_reference.student_unique_id,
        school_reference.school_id,
        {{ convert_grade_level_to_short_label('entry_grade_level_descriptor') }}     as grade_level,
        {{ convert_grade_level_to_id('entry_grade_level_descriptor') }}              as grade_level_id,
        row_number() over (
            partition by
                student_reference.student_unique_id,
                school_year_type_reference.school_year
            order by
                school_year_type_reference.school_year DESC,
                student_reference.student_unique_id,
                entry_date DESC
        ) as rank,
    from {{ ref('stg_edfi_student_school_associations') }}

),

student_grade_level as (

    select * from student_grade_level_ranked where rank = 1

),

students as (

    select distinct
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
            COALESCE(LEFT(students.middle_name, 1), '')
        )                                                               as student_display_name,
        seoa.electronic_mail[SAFE_OFFSET(0)].address                    as email,
        ifnull(active_enrollments.is_actively_enrolled_in_school, 'No')           as is_actively_enrolled_in_school,
        student_grade_level.grade_level                                 as grade_level,
        student_grade_level.grade_level_id                              as grade_level_id,
        COALESCE(
            seoa.limited_english_proficiency_descriptor,
            'Not applicable'
        )                                                               as limited_english_proficiency,
        if(
            seoa.limited_english_proficiency_descriptor = "Limited",
            "Yes",
            "No"
        )                                                               as is_english_language_learner,
        if (
            edfi_programs.program_name is not null,
            "Yes",
            "No"
        )                                                               as in_special_education_program,
        if(seoa.hispanic_latino_ethnicity is true, 'Yes', 'No')         as is_hispanic,
        case
            when seoa.hispanic_latino_ethnicity is true then 'Hispanic or Latino'
            when ARRAY_LENGTH(seoa.races) > 1 then 'Two or more races'
            when ARRAY_LENGTH(seoa.races) = 0 then 'Unknown'
            else seoa.races[OFFSET(0)].race_descriptor
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
    left join active_enrollments
        on students.student_unique_id = active_enrollments.student_unique_id
        and students.school_year = active_enrollments.school_year
    left join student_grade_level
        on students.student_unique_id = student_grade_level.student_unique_id
        and students.school_year = student_grade_level.school_year

)


select
    student_key,
    school_year,
    student_unique_id,
    student_first_name,
    student_middle_name,
    student_last_surname,
    student_display_name,
    email,
    is_actively_enrolled_in_school,
    grade_level,
    grade_level_id,
    ifnull(race_and_ethnicity_roll_up, 'Unknown') as race_and_ethnicity_roll_up,
    gender,
    birth_date,
    MIN(limited_english_proficiency)    as limited_english_proficiency,
    max(is_english_language_learner)    as is_english_language_learner,
    max(in_special_education_program)   as in_special_education_program,
    max(is_hispanic)                    as is_hispanic
from students
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
