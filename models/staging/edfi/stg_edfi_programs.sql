
WITH records AS (

    SELECT *
    FROM {{ source('staging', 'base_edfi_programs') }}
    WHERE date_extracted >= (
        SELECT MAX(date_extracted) AS date_extracted
        FROM {{ source('staging', 'base_edfi_programs') }}
        WHERE is_complete_extract IS TRUE)

)


SELECT
    date_extracted                          AS date_extracted,
    school_year                             AS school_year,
    id                                      AS id,
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
FROM records
WHERE
    extract_type = 'records'
    AND id NOT IN (SELECT id FROM records WHERE extract_type = 'deletes') 
QUALIFY ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY date_extracted DESC) = 1
