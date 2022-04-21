
{{ retrieve_edfi_records_from_data_lake('base_edfi_assessments') }}

select
    date_extracted                          as date_extracted,
    school_year                             as school_year,
    id                                      as id,
    json_value(data, '$.assessmentIdentifier') as assessment_identifier,
    json_value(data, '$.assessmentFamily') as assessment_family,
    json_value(data, '$.assessmentForm') as assessment_form,
    json_value(data, '$.assessmentTitle') as assessment_title,
    cast(json_value(data, '$.assessmentVersion') as int64) as assessment_version,
    cast(json_value(data, '$.maxRawScore') as float64) as max_raw_score,
    json_value(data, '$.namespace') as namespace,
    json_value(data, '$.nomenclature') as nomenclature,
    cast(json_value(data, "$.adaptiveAssessment") as BOOL) as adaptive_assessment,
    split(json_value(data, "$.assessmentCategoryDescriptor"), '#')[OFFSET(1)] as assessment_category_descriptor,
    parse_date('%Y-%m-%d', json_value(data, "$.revisionDate")) as revision_date,
    struct(
        json_value(data, '$.educationOrganizationReference.educationOrganizationId') as education_organization_id
    ) as education_organization_reference,
    array(
        select as struct 
            split(json_value(academic_subjects, "$.academicSubjectDescriptor"), '#')[OFFSET(1)] as academic_subject_descriptor,
        from unnest(json_query_array(data, "$.academicSubjects")) academic_subjects 
    ) as academic_subjects,
    array(
        select as struct 
            split(json_value(grade_levels, "$.gradeLevelDescriptor"), '#')[OFFSET(1)] as grade_level_descriptor
        from unnest(json_query_array(data, "$.assessedGradeLevels")) grade_levels 
    ) as assessed_grade_levels,
    array(
        select as struct 
            split(json_value(codes, "$.assessmentIdentificationSystemDescriptor"), '#')[OFFSET(1)] as assessment_identification_system_descriptor,
            json_value(codes, "$.assigningOrganizationIdentificationCode") as assigning_organization_identification_code,
            json_value(codes, "$.identificationCode") as identification_code
        from unnest(json_query_array(data, "$.identificationCodes")) codes 
    ) as identification_codes,
    array(
        select as struct 
            split(json_value(languages, "$.languageDescriptor"), '#')[OFFSET(1)] as language_descriptor,
        from unnest(json_query_array(data, "$.languages")) languages 
    ) as languages,
    array(
        select as struct 
            split(json_value(performance_levels, "$.assessmentReportingMethodDescriptor"), '#')[OFFSET(1)] as assessment_reporting_method_descriptor,
            split(json_value(performance_levels, "$.performanceLevelDescriptor"), '#')[OFFSET(1)] as performance_level_descriptor,
            split(json_value(performance_levels, "$.resultDatatypeTypeDescriptor"), '#')[OFFSET(1)] as result_datatype_type_descriptor,
            json_value(performance_levels, "$.maximumScore") as maximum_score,
            json_value(performance_levels, "$.minimumScore") as minimum_score
        from unnest(json_query_array(data, "$.performanceLevels")) performance_levels 
    ) as performance_levels,
    array(
        select as struct 
            split(json_value(period, "$.assessmentPeriodDescriptor"), '#')[OFFSET(1)] as assessment_period_descriptor,
            parse_date('%Y-%m-%d', json_value(period, "$.beginDate")) as begin_date,
            parse_date('%Y-%m-%d', json_value(period, "$.endDate")) as end_date
        from unnest(json_query_array(data, "$.period")) period 
    ) as period,
    array(
        select as struct 
            split(json_value(platform_types, "$.platformTypeDescriptor"), '#')[OFFSET(1)] as platform_type_descriptor,
        from unnest(json_query_array(data, "$.platformTypes")) platform_types 
    ) as platform_types,
    array(
        select as struct 
            split(json_value(scores, "$.assessmentReportingMethodDescriptor"), '#')[OFFSET(1)] as assessment_reporting_method_descriptor,
            split(json_value(scores, "$.resultDatatypeTypeDescriptor"), '#')[OFFSET(1)] as result_datatype_type_descriptor,
            json_value(scores, "$.maximumScore") as maximum_score,
            json_value(scores, "$.minimumScore") as minimum_score
        from unnest(json_query_array(data, "$.scores")) scores 
    ) as scores,
    array(
        select as struct
            json_value(sections, '$.sectionReference.localCourseCode') as local_course_code,
            json_value(sections, '$.sectionReference.schoolId') as school_id,
            json_value(sections, '$.sectionReference.schoolYear') as school_year,
            json_value(sections, '$.sectionReference.sectionIdentifier') as section_identifier,
            json_value(sections, '$.sectionReference.sessionName') as session_name
        from unnest(json_query_array(data, "$.sections")) sections 
    ) as sections
from records

{{ remove_edfi_deletes_and_duplicates() }}
