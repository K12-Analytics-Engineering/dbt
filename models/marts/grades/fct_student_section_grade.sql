{{
  config(
    labels = {'analytics_middle_tier': 'yes'}
  )
}}


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
        'grades.grading_period_reference.grading_period_descriptor',
        'grades.grading_period_reference.period_sequence'
    ]) }}                                                                   AS grading_period_key,
    {{ dbt_utils.surrogate_key([
        'student_section_association_reference.school_id',
        'grading_period_reference.school_year',
        'student_section_association_reference.session_name',
        'student_section_association_reference.local_course_code',
        'student_section_association_reference.section_identifier',
        'student_section_association_reference.student_unique_id',
        'student_section_association_reference.begin_date'
    ]) }}                                                                   AS student_section_key,
    {{ dbt_utils.surrogate_key([
        'student_section_association_reference.school_id',
        'student_section_association_reference.school_year',
        'student_section_association_reference.session_name',
        'student_section_association_reference.local_course_code',
        'student_section_association_reference.section_identifier'
    ]) }}                                                                   AS section_key,
    grading_period_reference.school_year                                    AS school_year,
    numeric_grade_earned                                                    AS numeric_grade_earned,
    letter_grade_earned                                                     AS letter_grade_earned,
    grade_type_descriptor                                                   AS grade_type
FROM {{ ref('stg_edfi_grades') }} grades
