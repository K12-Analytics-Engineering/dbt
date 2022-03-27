SELECT NULL AS column1
{# 
WITH parsed_data AS (

    SELECT
        date_extracted                          AS date_extracted,
        school_year                             AS school_year,
        JSON_VALUE(data, '$.id') AS id,
        JSON_VALUE(data, '$.questionCode') AS question_code,
        JSON_VALUE(data, '$.questionText') AS question_text,
        SPLIT(JSON_VALUE(data, '$.questionFormDescriptor'), '#')[OFFSET(1)] AS question_form_descriptor,
        ARRAY(
            SELECT AS STRUCT 
                JSON_VALUE(response_choices, "$.textValue") AS text_value,
                CAST(JSON_VALUE(response_choices, "$.sortOrder") AS int64) AS sort_order,
                CAST(JSON_VALUE(response_choices, "$.numericValue") AS int64) AS numeric_value
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.responseChoices")) response_choices 
        ) AS response_choices,
        STRUCT(
            JSON_VALUE(data, '$.surveyReference.namespace') AS namespace,
            JSON_VALUE(data, '$.surveyReference.surveyIdentifier') AS survey_identifier
        ) AS survey_reference,
        STRUCT(
            JSON_VALUE(data, '$.surveySectionReference.namespace') AS namespace,
            JSON_VALUE(data, '$.surveySectionReference.surveyIdentifier') AS survey_identifier,
            JSON_VALUE(data, '$.surveySectionReference.surveySectionTitle') AS survey_section_title
        ) AS survey_section_reference,
        ARRAY(
            SELECT AS STRUCT 
                JSON_VALUE(matrices, "$.matrixElement") AS matrix_element,
                CAST(JSON_VALUE(matrices, "$.minRawScore") AS int64) AS min_raw_score,
                CAST(JSON_VALUE(matrices, "$.maxRawScore") AS int64) AS max_raw_score
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.matrices")) matrices 
        ) AS matrices
    FROM {{ source('staging', 'base_edfi_survey_questions') }}
    WHERE date_extracted >= (
        SELECT MAX(date_extracted) AS date_extracted
        FROM {{ source('staging', 'base_edfi_survey_questions') }}
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
