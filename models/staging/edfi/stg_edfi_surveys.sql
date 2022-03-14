
WITH parsed_data AS (

    SELECT
        CAST(JSON_VALUE(data, '$.extractedTimestamp') AS TIMESTAMP) AS extracted_timestamp,
        JSON_VALUE(data, '$.id') AS id,
        CAST(JSON_VALUE(data, '$.schoolYear') AS int64) school_year,
        JSON_VALUE(data, '$.namespace') AS namespace,
        JSON_VALUE(data, '$.surveyIdentifier') AS survey_identifier,
        JSON_VALUE(data, '$.surveyTitle') AS survey_title,
        STRUCT(
            JSON_VALUE(data, '$.educationOrganizationReference.educationOrganizationId') AS education_organization_id
        ) AS education_organization_reference,
        STRUCT(
            CAST(JSON_VALUE(data, '$.schoolYearTypeReference.schoolYear') AS int64) AS school_year
        ) AS school_year_type_reference,
        STRUCT(
            JSON_VALUE(data, '$.sessionReference.schoolId') AS school_id,
            CAST(JSON_VALUE(data, '$.sessionReference.schoolYear') AS int64) AS school_year,
            JSON_VALUE(data, '$.sessionReference.sessionName') AS session_name
        ) AS session_reference,
        CAST(JSON_VALUE(data, '$.numberAdministered') AS int64) number_administered,
        SPLIT(JSON_VALUE(data, '$.surveyCategoryDescriptor'), '#')[OFFSET(1)] AS survey_category_descriptor
    FROM {{ source('staging', 'base_edfi_surveys') }}
    QUALIFY ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY extracted_timestamp DESC) = 1

)


SELECT *
FROM parsed_data
WHERE
    id NOT IN (
        SELECT id FROM {{ ref('stg_edfi_deletes') }} edfi_deletes
        WHERE parsed_data.school_year = edfi_deletes.school_year)
