
WITH parsed_data AS (

    SELECT
        JSON_VALUE(data, '$.extractedTimestamp') AS extracted_timestamp,
        JSON_VALUE(data, '$.id') AS id,
        CAST(JSON_VALUE(data, '$.schoolYear') AS int64) school_year,
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

),

ranked AS (

    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY
                school_year,
                survey_reference.survey_identifier,
                survey_reference.namespace,
                survey_response_identifier
            ORDER BY school_year DESC, extracted_timestamp DESC
        ) AS rank,
        *
    FROM parsed_data

)

SELECT * EXCEPT (extracted_timestamp, rank)
FROM ranked
WHERE
    rank = 1
    AND id NOT IN (
        SELECT id FROM {{ ref('stg_edfi_deletes') }} edfi_deletes
        WHERE ranked.school_year = edfi_deletes.school_year
    )
