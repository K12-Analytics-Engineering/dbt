
select
    {{ dbt_utils.surrogate_key([
        'ssa.section_reference.school_id',
        'ssa.section_reference.school_year'
    ]) }}                                               as school_key,
    {{ dbt_utils.surrogate_key([
        'course_offerings.session_reference.school_id',
        'course_offerings.session_reference.school_year',
        'course_offerings.session_reference.session_name'
    ]) }}                                               as session_key,
    {{ dbt_utils.surrogate_key([
        'ssa.section_reference.school_id',
        'ssa.section_reference.school_year',
        'ssa.section_reference.session_name',
        'ssa.section_reference.local_course_code',
        'ssa.section_reference.section_identifier'
    ]) }}                                               as section_key,
    {{ dbt_utils.surrogate_key([
        'ssa.section_reference.school_id',
        'ssa.section_reference.school_year',
        'ssa.section_reference.session_name',
        'ssa.section_reference.local_course_code',
        'ssa.section_reference.section_identifier'
    ]) }}                                               as staff_group_key,
    {{ dbt_utils.surrogate_key([
        'ssa.student_reference.student_unique_id',
        'ssa.section_reference.school_year'
     ]) }}                                              as student_key,
    section_reference.school_year                       as school_year,
    ssa.homeroom_indicator                              as homeroom_indicator,
    ssa.begin_date                                      as start_date,
    ssa.end_date                                        as end_date,
    if(
        ssa.begin_date is null
        or (
            current_date >= ssa.begin_date
            and current_date < ssa.end_date
        ),
        1, 0)                                           as is_actively_enrolled_in_section
from {{ ref('stg_edfi_student_section_associations') }} ssa
left join {{ ref('stg_edfi_course_offerings') }} course_offerings
    on ssa.school_year = course_offerings.school_year
    and course_offerings.local_course_code = ssa.section_reference.local_course_code
    and course_offerings.school_reference.school_id = ssa.section_reference.school_id
    and course_offerings.session_reference.school_year = ssa.section_reference.school_year
    and course_offerings.session_reference.session_name = ssa.section_reference.session_name
