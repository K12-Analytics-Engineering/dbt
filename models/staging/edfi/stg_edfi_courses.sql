
{{ retrieve_edfi_records_from_data_lake('base_edfi_courses') }}

select
    date_extracted                          as date_extracted,
    school_year                             as school_year,
    id                                      as id,
    json_value(data, '$.courseCode') as course_code,
    json_value(data, '$.courseTitle') as course_title,
    json_value(data, '$.courseDescription') as course_description,
    split(json_value(data, "$.academicSubjectDescriptor"), '#')[OFFSET(1)] as academic_subject_descriptor,
    split(json_value(data, "$.careerPathwayDescriptor"), '#')[OFFSET(1)] as career_pathway_descriptor,
    split(json_value(data, "$.courseDefinedByDescriptor"), '#')[OFFSET(1)] as course_defined_by_descriptor,
    split(json_value(data, "$.courseGPAApplicabilityDescriptor"), '#')[OFFSET(1)] as course_gpa_applicability_descriptor,
    parse_date('%Y-%m-%d', json_value(data, "$.dateCourseAdopted")) as date_course_adopted,
    cast(json_value(data, "$.highSchoolCourseRequirement") as BOOL) as high_school_course_requirement,
    cast(json_value(data, "$.maxCompletionsForCredit") as float64) as max_completions_for_credit,
    cast(json_value(data, "$.maximumAvailableCreditConversion") as float64) as maximum_available_credit_conversion,
    cast(json_value(data, "$.maximumAvailableCredits") as float64) as maximum_available_credits,
    cast(json_value(data, "$.minimumAvailableCreditConversion") as float64) as minimum_available_credit_conversion,
    cast(json_value(data, "$.minimumAvailableCredits") as float64) as minimum_available_credits,
    cast(json_value(data, "$.numberOfParts") as int64) as number_of_parts,
    struct(
        json_value(data, '$.educationOrganizationReference.educationOrganizationId') as education_organization_id
    ) as education_organization_reference,
    array(
        select as struct 
            split(json_value(levels, "$.competencyLevelDescriptor"), '#')[OFFSET(1)] as competency_level_descriptor,
        from unnest(json_query_array(data, "$.competencyLevels")) levels 
    ) as competency_levels,
    array(
        select as struct 
            split(json_value(codes, "$.identificationCodes.courseIdentificationSystemDescriptor"), '#')[OFFSET(1)] as course_identification_system_descriptor,
            split(json_value(codes, "$.identificationCodes.assigningOrganizationIdentificationCode"), '#')[OFFSET(1)] as assigning_organization_identification_code,
            json_value(codes, "$.identificationCodes.courseCatalogURL") as course_catalog_url,
            json_value(codes, "$.identificationCodes.identificationCode") as identification_code
        from unnest(json_query_array(data, "$.identificationCodes")) codes 
    ) as identification_codes
from records

{{ remove_edfi_deletes_and_duplicates() }}
