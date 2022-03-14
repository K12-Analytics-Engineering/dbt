

WITH parsed_data AS (

    SELECT
        JSON_VALUE(data, '$.extractedTimestamp') AS extracted_timestamp,
        JSON_VALUE(data, '$.id') AS id,
        CAST(JSON_VALUE(data, '$.schoolYear') AS int64) school_year,
        JSON_VALUE(data, '$.programName') AS program_name,
        JSON_VALUE(data, '$.programId') AS program_id,
        SPLIT(JSON_VALUE(data, '$.programTypeDescriptor'), '#')[OFFSET(1)] AS program_type_descriptor,
        STRUCT(
            JSON_VALUE(data, '$.educationOrganizationReference.educationOrganizationId') AS education_organization_id
        ) AS education_organization_reference,
        ARRAY(
            SELECT AS STRUCT 
                SPLIT(JSON_VALUE(services, "$.serviceDescriptor"), '#')[OFFSET(1)] AS service_descriptor,
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.services")) services 
        ) AS services,
        ARRAY(
            SELECT AS STRUCT 
                SPLIT(JSON_VALUE(sponsors, "$.programSponsorDescriptor"), '#')[OFFSET(1)] AS program_sponsor_descriptor,
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.sponsors")) sponsors 
        ) AS sponsors,
        ARRAY(
            SELECT AS STRUCT 
                SPLIT(JSON_VALUE(characteristics, "$.programCharacteristicDescriptor"), '#')[OFFSET(1)] AS program_characteristic_descriptor,
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.characteristics")) characteristics 
        ) AS characteristics,
        ARRAY(
            SELECT AS STRUCT
                STRUCT(
                    JSON_VALUE(learning_objectives, '$.learningObjectiveReference.learningObjectiveId') AS learning_objective_id,
                    JSON_VALUE(learning_objectives, '$.learningObjectiveReference.namespace') AS namespace
                ) AS learning_objective_reference
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.learningObjectives")) learning_objectives 
        ) AS learning_objectives,
        ARRAY(
                SELECT AS STRUCT
                    STRUCT(
                        JSON_VALUE(learning_standards, '$.learningStandardReference.learningStandardId') AS learning_standard_id
                    ) AS learning_standard_reference
                FROM UNNEST(JSON_QUERY_ARRAY(data, "$.learningStandards")) learning_standards
        ) AS learning_standards,
        JSON_VALUE(data, '$.schoolId') AS school_id,
        JSON_VALUE(data, '$.nameOfInstitution') AS name_of_institution,
        SPLIT(JSON_VALUE(data, '$.schoolTypeDescriptor'), '#')[OFFSET(1)] AS school_type_descriptor,
    FROM {{ source('staging', 'base_edfi_programs') }}
    QUALIFY ROW_NUMBER() OVER (
            PARTITION BY
                school_year,
                education_organization_reference.education_organization_id,
                program_name,
                program_type_descriptor
            ORDER BY school_year DESC, extracted_timestamp DESC) = 1

)


SELECT *
FROM parsed_data
WHERE
    id NOT IN (
        SELECT id FROM {{ ref('stg_edfi_deletes') }} edfi_deletes
        WHERE parsed_data.school_year = edfi_deletes.school_year)
