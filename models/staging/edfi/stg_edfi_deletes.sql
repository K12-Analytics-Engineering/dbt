
{# 
Add tables below if using survey endpoints:
"base_edfi_surveys_deletes",
"base_edfi_survey_questions_deletes",
"base_edfi_survey_responses_deletes",
"base_edfi_survey_question_responses_deletes", #}

{%
    set tables = [
        "base_edfi_assessments_deletes",
        "base_edfi_local_education_agencies_deletes",
        "base_edfi_objective_assessments_deletes",
        "base_edfi_schools_deletes",
        "base_edfi_students_deletes",
        "base_edfi_student_education_organization_associations_deletes",
        "base_edfi_student_school_associations_deletes",
        "base_edfi_calendars_deletes",
        "base_edfi_calendars_deletes",
        "base_edfi_calendar_dates_deletes",
        "base_edfi_courses_deletes",
        "base_edfi_course_offerings_deletes",
        "base_edfi_discipline_actions_deletes",
        "base_edfi_discipline_incident_deletes",
        "base_edfi_grades_deletes",
        "base_edfi_grading_periods_deletes",
        "base_edfi_staff_discipline_incident_associations_deletes",
        "base_edfi_student_discipline_incident_associations_deletes",
        "base_edfi_parents_deletes",
        "base_edfi_programs_deletes",
        "base_edfi_sections_deletes",
        "base_edfi_staffs_deletes",
        "base_edfi_staff_education_organization_assignment_associations_deletes",
        "base_edfi_staff_school_associations_deletes",
        "base_edfi_staff_section_associations_deletes",
        "base_edfi_student_assessments_deletes",
        "base_edfi_student_parent_associations_deletes",
        "base_edfi_student_program_associations_deletes",
        "base_edfi_student_school_attendance_events_deletes",
        "base_edfi_student_section_associations_deletes",
        "base_edfi_student_section_attendance_events_deletes",
        "base_edfi_student_special_education_program_associations_deletes",
        "base_edfi_sessions_deletes",
        "base_edfi_cohort_type_descriptors_deletes",
        "base_edfi_disability_descriptors_deletes",
        "base_edfi_language_descriptors_deletes",
        "base_edfi_language_use_descriptors_deletes",
        "base_edfi_race_descriptors_deletes"
    ]
%}


{% for table in tables %}

    SELECT
        date_extracted                          AS date_extracted,
        school_year                             AS school_year,
        JSON_VALUE(data, '$.Id')                AS id,
        JSON_VALUE(data, '$.ChangeVersion')     AS change_version
    FROM {{ source('staging', table) }}
    {% if not loop.last %} UNION ALL {% endif %}

{% endfor %}
