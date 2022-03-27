SELECT NULL AS column1
{# 
WITH parsed_data AS (

    SELECT
        date_extracted                          AS date_extracted,
        school_year                             AS school_year,
        JSON_VALUE(data, '$.id') AS id,
        JSON_VALUE(data, '$.surveyResponseIdentifier') AS survey_response_identifier,
        STRUCT(
            JSON_VALUE(data, '$.parentReference.parentUniqueId') AS parent_unique_id
        ) AS parent_reference,
        STRUCT(
            JSON_VALUE(data, '$.staffReference.staffUniqueId') AS staff_unique_id
        ) AS staff_reference,
        STRUCT(
            JSON_VALUE(data, '$.studentReference.studentUniqueId') AS student_unique_id
        ) AS student_reference,
        STRUCT(
            JSON_VALUE(data, '$.surveyReference.namespace') AS namespace,
            JSON_VALUE(data, '$.surveyReference.surveyIdentifier') AS survey_identifier
        ) AS survey_reference,
        JSON_VALUE(data, '$.electronicMailAddress') AS electronic_mail_address,
        JSON_VALUE(data, '$.fullName') AS full_name,
        JSON_VALUE(data, '$.location') AS location,
        PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, "$.responseDate")) AS response_date,
        CAST(JSON_VALUE(data, "$.responseTime") AS int64) AS response_time,
        ARRAY(
            SELECT AS STRUCT 
                SPLIT(JSON_VALUE(survey_levels, '$.surveyLevelDescriptor'), '#')[OFFSET(1)] AS survey_level_descriptor
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.surveyLevels")) survey_levels 
        ) AS survey_levels
    FROM {{ source('staging', 'base_edfi_survey_responses') }}
    WHERE date_extracted >= (
        SELECT MAX(date_extracted) AS date_extracted
        FROM {{ source('staging', 'base_edfi_survey_responses') }}
        WHERE is_complete_extract IS TRUE)
    QUALIFY ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY date_extracted DESC) = 1

)


SELECT *
FROM parsed_data
WHERE
    id NOT IN (
        SELECT id FROM {{ ref('stg_edfi_deletes') }} edfi_deletes
        WHERE parsed_data.school_year = edfi_deletes.school_year) #}
