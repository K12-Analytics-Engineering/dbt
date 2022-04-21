
with associations as (

    -- if staff and actively assigned to school with
    -- staff classification of Superintendent, School Administrator, or Principal,
    -- associate staff with all students with any enrollment at school
    select distinct
        fct_staff_school.school_year    as school_year,
        dim_staff.email                 as user_email,
        dim_student.student_unique_id   as student_unique_id
    from {{ ref('fct_staff_school') }} fct_staff_school
    left join {{ ref('dim_staff') }} dim_staff
        on fct_staff_school.staff_key = dim_staff.staff_key
    left join {{ ref('fct_student_school') }} fct_student_school
        on fct_staff_school.school_key = fct_student_school.school_key
    left join {{ ref('dim_student') }} dim_student
        on fct_student_school.student_key = dim_student.student_key
    where
        fct_staff_school.is_actively_assigned_to_school = 1
        and fct_staff_school.staff_classification in (
            'Superintendent',
            'School Administrator',
            'Principal')


    union all


    select  
        fct_student_section.school_year     as school_year,
        dim_staff.email                     as user_email,
        dim_student.student_unique_id       as student_unique_id
    from {{ ref('fct_student_section') }} fct_student_section
    left join {{ ref('dim_student') }} dim_student
        on fct_student_section.student_key = dim_student.student_key
    left join {{ ref('bridge_staff_group') }} bridge_staff_group
        on fct_student_section.staff_group_key = bridge_staff_group.staff_group_key
    left join {{ ref('dim_staff') }} dim_staff
        on bridge_staff_group.staff_key = dim_staff.staff_key


    union all


    select
        school_year         as school_year,
        email               as user_email,
        student_unique_id   as student_unique_id
    from {{ ref('dim_student') }} dim_student

),

distinct_values as (

    select distinct
        school_year,
        student_unique_id,
        user_email,
    from associations
    where user_email is not null

)


select
    {{ dbt_utils.surrogate_key([
            'student_unique_id',
            'school_year'
    ]) }}                                   as student_key,
    ARRAY_AGG(user_email)                   as authorized_emails
from distinct_values
group by 1
