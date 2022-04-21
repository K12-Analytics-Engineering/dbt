
select
    {{ dbt_utils.surrogate_key([
        'staff_section_associations.section_reference.school_id',
        'staff_section_associations.section_reference.school_year',
        'staff_section_associations.section_reference.session_name',
        'staff_section_associations.section_reference.local_course_code',
        'staff_section_associations.section_reference.section_identifier'
    ]) }}                                                                       as staff_group_key,
    {{ dbt_utils.surrogate_key([
        'staff_section_associations.staff_reference.staff_unique_id',
        'staff_section_associations.section_reference.school_year'
    ]) }}                                                                       as staff_key,
    staff_section_associations.classroom_position_descriptor                    as classroom_position,
    staff_section_associations.highly_qualified_teacher                         as highly_qualified_teacher,
    staff_section_associations.percentage_contribution                          as percentage_contribution
from {{ ref('stg_edfi_staff_section_associations') }} staff_section_associations
left join {{ ref('stg_edfi_sections') }} sections
    on staff_section_associations.section_reference.local_course_code = sections.course_offering_reference.local_course_code
    and staff_section_associations.section_reference.school_id = sections.course_offering_reference.school_id
    and staff_section_associations.section_reference.school_year = sections.course_offering_reference.school_year
    and staff_section_associations.section_reference.section_identifier = sections.section_identifier
    and staff_section_associations.section_reference.session_name = sections.course_offering_reference.session_name
left join {{ ref('stg_edfi_course_offerings') }} course_offerings
    on sections.course_offering_reference.local_course_code = course_offerings.local_course_code
    and sections.course_offering_reference.school_id = course_offerings.school_reference.school_id
    and sections.course_offering_reference.school_year = course_offerings.session_reference.school_year
    and sections.course_offering_reference.session_name = course_offerings.session_reference.session_name
left join {{ ref('stg_edfi_sessions') }} sessions
    on course_offerings.school_reference.school_id = sessions.school_reference.school_id
    and course_offerings.session_reference.school_year = sessions.school_year_type_reference.school_year
    and course_offerings.session_reference.session_name = sessions.session_name
where
    current_date between staff_section_associations.begin_date and staff_section_associations.end_date
    or staff_section_associations.end_date = sessions.end_date
