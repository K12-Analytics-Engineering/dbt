{{
  config(
    labels = {'analytics_middle_tier': 'yes'}
  )
}}


SELECT
    {{ dbt_utils.surrogate_key([
        'sections.course_offering_reference.school_id',
        'sections.course_offering_reference.school_year',
        'sections.course_offering_reference.session_name',
        'sections.course_offering_reference.local_course_code',
        'sections.section_identifier'
    ]) }}                                                       AS section_key,
    {{ dbt_utils.surrogate_key([
        'stg_edfi_schools.local_education_agency_id',
        'sections.course_offering_reference.school_year'
    ]) }}                                                       AS local_education_agency_key,
    {{ dbt_utils.surrogate_key([
        'sections.course_offering_reference.school_id',
        'sections.course_offering_reference.school_year'
    ]) }}                                                       AS school_key,
    {{ dbt_utils.surrogate_key([
        'sections.course_offering_reference.school_id',
        'sections.course_offering_reference.school_year',
        'sections.course_offering_reference.session_name'
    ]) }}                                                       AS session_key,
    sections.section_identifier                                 AS section_identifier,
    COALESCE(
        sections.section_name,
        CONCAT(
            course_offering_reference.local_course_code, "-",
            sessions.session_name
        )
    )                                                           AS section_name,
    sessions.session_name                                       AS session_name,
    course_offering_reference.local_course_code                 AS local_course_code,
    courses.course_title                                        AS course_title,
    courses.course_gpa_applicability_descriptor                 AS course_gpa_applicability,
    course_offerings.session_reference.school_year              AS school_year,
    sections.available_credits                                  AS available_credits
FROM {{ ref('stg_edfi_sections') }} sections
CROSS JOIN UNNEST(sections.class_periods) AS class_period
LEFT JOIN {{ ref('stg_edfi_schools') }} stg_edfi_schools
    ON sections.course_offering_reference.school_id = stg_edfi_schools.school_id
LEFT JOIN {{ ref('stg_edfi_course_offerings') }} course_offerings
    ON sections.school_year = course_offerings.school_year
    AND course_offerings.local_course_code = sections.course_offering_reference.local_course_code
    AND course_offerings.school_reference.school_id = sections.course_offering_reference.school_id
    AND course_offerings.session_reference.school_year = sections.course_offering_reference.school_year
    AND course_offerings.session_reference.session_name = sections.course_offering_reference.session_name
LEFT JOIN {{ ref('stg_edfi_courses') }} courses
    ON course_offerings.school_year = courses.school_year
    AND courses.course_code = course_offerings.course_reference.course_code
    AND courses.education_organization_reference.education_organization_id = course_offerings.course_reference.education_organization_id
LEFT JOIN {{ ref('stg_edfi_sessions') }} sessions
    ON course_offerings.session_reference.school_id = sessions.school_reference.school_id
    AND course_offerings.session_reference.school_year = sessions.school_year_type_reference.school_year
    AND course_offerings.session_reference.session_name = sessions.session_name
