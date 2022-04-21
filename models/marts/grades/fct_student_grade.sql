
select
    {{ dbt_utils.surrogate_key([
        'grades.student_section_association_reference.school_id',
        'grades.grading_period_reference.school_year'
    ]) }}                                                                   as school_key,
    {{ dbt_utils.surrogate_key([
        'grades.student_section_association_reference.student_unique_id',
        'grades.grading_period_reference.school_year'
    ]) }}                                                                   as student_key,
    {{ dbt_utils.surrogate_key([
        'grades.grading_period_reference.school_id',
        'grades.grading_period_reference.school_year',
        'student_section_association_reference.session_name',
        'grades.grading_period_reference.grading_period_descriptor',
        'grades.grading_period_reference.period_sequence'
    ]) }}                                                                   as grading_period_key,
    {{ dbt_utils.surrogate_key([
        'student_section_association_reference.school_id',
        'student_section_association_reference.school_year',
        'student_section_association_reference.session_name',
        'student_section_association_reference.local_course_code',
        'student_section_association_reference.section_identifier'
    ]) }}                                                                   as section_key,
    {{ dbt_utils.surrogate_key([
        'student_section_association_reference.school_id',
        'student_section_association_reference.school_year',
        'student_section_association_reference.session_name',
        'student_section_association_reference.local_course_code',
        'student_section_association_reference.section_identifier'
    ]) }}                                                                   as staff_group_key,
    grading_period_reference.school_year                                    as school_year,
    numeric_grade_earned                                                    as numeric_grade_earned,
    letter_grade_earned                                                     as letter_grade_earned,
    grade_type_descriptor                                                   as grade_type,
    if(current_date between ssa.begin_date and ssa.end_date, 1, 0)          as is_actively_enrolled_in_section
from {{ ref('stg_edfi_grades') }} grades
left join {{ ref('stg_edfi_student_section_associations') }} ssa
    on grades.school_year = ssa.school_year
    and grades.student_section_association_reference.student_unique_id = ssa.student_reference.student_unique_id
    and grades.student_section_association_reference.begin_date = ssa.begin_date
    and grades.student_section_association_reference.local_course_code = ssa.section_reference.local_course_code
    and grades.student_section_association_reference.school_id = ssa.section_reference.school_id
    and grades.student_section_association_reference.school_year = ssa.section_reference.school_year
    and grades.student_section_association_reference.session_name = ssa.section_reference.session_name
