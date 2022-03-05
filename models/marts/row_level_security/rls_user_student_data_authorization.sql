
WITH associations AS (

    -- if staff and actively assigned to school with
    -- staff classification of Superintendent, School Administrator, or Principal,
    -- associate staff with all students with any enrollment at school
    SELECT DISTINCT
        fct_staff_school.school_year    AS school_year,
        dim_staff.email                 AS user_email,
        dim_student.student_unique_id   AS student_unique_id
    FROM {{ ref('fct_staff_school') }} fct_staff_school
    LEFT JOIN {{ ref('dim_staff') }} dim_staff
        ON fct_staff_school.staff_key = dim_staff.staff_key
    LEFT JOIN {{ ref('fct_student_school') }} fct_student_school
        ON fct_staff_school.school_key = fct_student_school.school_key
    LEFT JOIN {{ ref('dim_student') }} dim_student
        ON fct_student_school.student_key = dim_student.student_key
    WHERE
        fct_staff_school.is_actively_assigned_to_school = 1
        AND fct_staff_school.staff_classification IN (
            'Superintendent',
            'School Administrator',
            'Principal')


    UNION ALL


    SELECT  
        fct_student_section.school_year     AS school_year,
        dim_staff.email                     AS user_email,
        dim_student.student_unique_id       AS student_unique_id
    FROM {{ ref('fct_student_section') }} fct_student_section
    LEFT JOIN {{ ref('dim_student') }} dim_student
        ON fct_student_section.student_key = dim_student.student_key
    LEFT JOIN {{ ref('bridge_staff_group') }} bridge_staff_group
        ON fct_student_section.staff_group_key = bridge_staff_group.staff_group_key
    LEFT JOIN {{ ref('dim_staff') }} dim_staff
        ON bridge_staff_group.staff_key = dim_staff.staff_key


    UNION ALL


    SELECT
        school_year         AS school_year,
        email               AS user_email,
        student_unique_id   AS student_unique_id
    FROM {{ ref('dim_student') }} dim_student

)

SELECT DISTINCT
    school_year,
    user_email,
    student_unique_id
FROM associations
WHERE user_email IS NOT NULL
