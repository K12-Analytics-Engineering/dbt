
WITH parsed_data AS (

    SELECT
        JSON_VALUE(data, '$.extractedTimestamp') AS extracted_timestamp,
        JSON_VALUE(data, '$.id') AS id,
        CAST(JSON_VALUE(data, '$.schoolYear') AS int64) school_year,
        JSON_VALUE(data, '$.courseCode') AS course_code,
        JSON_VALUE(data, '$.courseTitle') AS course_title,
        JSON_VALUE(data, '$.courseDescription') AS course_description,
        SPLIT(JSON_VALUE(data, "$.academicSubjectDescriptor"), '#')[OFFSET(1)] AS academic_subject_descriptor,
        SPLIT(JSON_VALUE(data, "$.careerPathwayDescriptor"), '#')[OFFSET(1)] AS career_pathway_descriptor,
        SPLIT(JSON_VALUE(data, "$.courseDefinedByDescriptor"), '#')[OFFSET(1)] AS course_defined_by_descriptor,
        SPLIT(JSON_VALUE(data, "$.courseGPAApplicabilityDescriptor"), '#')[OFFSET(1)] AS course_gpa_applicability_descriptor,
        PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, "$.dateCourseAdopted")) AS date_course_adopted,
        CAST(JSON_VALUE(data, "$.highSchoolCourseRequirement") AS BOOL) AS high_school_course_requirement,
        CAST(JSON_VALUE(data, "$.maxCompletionsForCredit") AS float64) AS max_completions_for_credit,
        CAST(JSON_VALUE(data, "$.maximumAvailableCreditConversion") AS float64) AS maximum_available_credit_conversion,
        CAST(JSON_VALUE(data, "$.maximumAvailableCredits") AS float64) AS maximum_available_credits,
        CAST(JSON_VALUE(data, "$.minimumAvailableCreditConversion") AS float64) AS minimum_available_credit_conversion,
        CAST(JSON_VALUE(data, "$.minimumAvailableCredits") AS float64) AS minimum_available_credits,
        CAST(JSON_VALUE(data, "$.numberOfParts") AS int64) AS number_of_parts,
        STRUCT(
            JSON_VALUE(data, '$.educationOrganizationReference.educationOrganizationId') AS education_organization_id
        ) AS education_organization_reference,
        ARRAY(
            SELECT AS STRUCT 
                SPLIT(JSON_VALUE(levels, "$.competencyLevelDescriptor"), '#')[OFFSET(1)] AS competency_level_descriptor,
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.competencyLevels")) levels 
        ) AS competency_levels,
        ARRAY(
            SELECT AS STRUCT 
                SPLIT(JSON_VALUE(codes, "$.identificationCodes.courseIdentificationSystemDescriptor"), '#')[OFFSET(1)] AS course_identification_system_descriptor,
                SPLIT(JSON_VALUE(codes, "$.identificationCodes.assigningOrganizationIdentificationCode"), '#')[OFFSET(1)] AS assigning_organization_identification_code,
                JSON_VALUE(codes, "$.identificationCodes.courseCatalogURL") AS course_catalog_url,
                JSON_VALUE(codes, "$.identificationCodes.identificationCode") AS identification_code
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.identificationCodes")) codes 
        ) AS identification_codes
    FROM {{ source('staging', 'base_edfi_courses') }}
    QUALIFY ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY extracted_timestamp DESC) = 1

)


SELECT *
FROM parsed_data
WHERE
    id NOT IN (
        SELECT id FROM {{ ref('stg_edfi_deletes') }} edfi_deletes
        WHERE ranked.school_year = edfi_deletes.school_year)
