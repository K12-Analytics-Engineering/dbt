
SELECT
    {{ dbt_utils.surrogate_key([
        'grades.student_section_association_reference.school_id',
        'grades.grading_period_reference.school_year'
    ]) }}                                                                   AS school_key,
    {{ dbt_utils.surrogate_key([
        'grades.student_section_association_reference.student_unique_id',
        'grades.grading_period_reference.school_year'
    ]) }}                                                                   AS student_key,
    {{ dbt_utils.surrogate_key([
        'grades.grading_period_reference.school_id',
        'grades.grading_period_reference.school_year',
        'student_section_association_reference.session_name',
        'grades.grading_period_reference.grading_period_descriptor',
        'grades.grading_period_reference.period_sequence'
    ]) }}                                                                   AS grading_period_key,
    {{ dbt_utils.surrogate_key([
        'student_section_association_reference.school_id',
        'student_section_association_reference.school_year',
        'student_section_association_reference.session_name',
        'student_section_association_reference.local_course_code',
        'student_section_association_reference.section_identifier'
    ]) }}                                                                   AS section_key,
    {{ dbt_utils.surrogate_key([
        'student_section_association_reference.school_id',
        'student_section_association_reference.school_year',
        'student_section_association_reference.session_name',
        'student_section_association_reference.local_course_code',
        'student_section_association_reference.section_identifier'
    ]) }}                                                                   AS staff_group_key,
    grading_period_reference.school_year                                    AS school_year,
    numeric_grade_earned                                                    AS numeric_grade_earned,
    letter_grade_earned                                                     AS letter_grade_earned,
    grade_type_descriptor                                                   AS grade_type,
    IF(CURRENT_DATE BETWEEN ssa.begin_date AND ssa.end_date, 1, 0)          AS is_actively_enrolled_in_section
FROM {{ ref('stg_edfi_grades') }} grades
LEFT JOIN {{ ref('stg_edfi_student_section_associations') }} ssa
    ON grades.school_year = ssa.school_year
    AND grades.student_section_association_reference.student_unique_id = ssa.student_reference.student_unique_id
    AND grades.student_section_association_reference.begin_date = ssa.begin_date
    AND grades.student_section_association_reference.local_course_code = ssa.section_reference.local_course_code
    AND grades.student_section_association_reference.school_id = ssa.section_reference.school_id
    AND grades.student_section_association_reference.school_year = ssa.section_reference.school_year
    AND grades.student_section_association_reference.session_name = ssa.section_reference.session_name
