SELECT NULL AS column1
{# 
WITH parsed_data AS (

    SELECT
        date_extracted                          AS date_extracted,
        school_year                             AS school_year,
        JSON_VALUE(data, '$.id') AS id,
        STRUCT(
            JSON_VALUE(data, '$.surveyQuestionReference.namespace') AS namespace,
            JSON_VALUE(data, '$.surveyQuestionReference.questionCode') AS question_code,
            JSON_VALUE(data, '$.surveyQuestionReference.surveyIdentifier') AS survey_identifier
        ) AS survey_question_reference,
        STRUCT(
            JSON_VALUE(data, '$.surveyResponseReference.namespace') AS namespace,
            JSON_VALUE(data, '$.surveyResponseReference.surveyIdentifier') AS survey_identifier,
            JSON_VALUE(data, '$.surveyResponseReference.surveyResponseIdentifier') AS survey_response_identifier
        ) AS survey_response_reference,
        JSON_VALUE(data, '$.comment') AS comment,
        CAST(JSON_VALUE(data, '$.noResponse') AS BOOL) AS no_response,
        ARRAY(
            SELECT AS STRUCT
                JSON_VALUE(matrix_response, '$.matrixElement') AS matrix_element,
                JSON_VALUE(matrix_response, '$.textResponse') AS text_response,
                CAST(JSON_VALUE(matrix_response, '$.minNumericResponse') AS int64) AS min_numeric_response,
                CAST(JSON_VALUE(matrix_response, '$.maxNumericResponse') AS int64) AS max_numeric_response,
                CAST(JSON_VALUE(matrix_response, '$.numericResponse') AS int64) AS numeric_response,
                CAST(JSON_VALUE(matrix_response, '$.noResponse') AS BOOL) AS no_response
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.surveyQuestionMatrixElementResponses")) matrix_response 
        ) AS survey_question_matrix_element_responses,
        ARRAY(
            SELECT AS STRUCT
                JSON_VALUE(value, '$.surveyQuestionResponseValueIdentifier') AS survey_question_response_value_identifier,
                CAST(JSON_VALUE(value, '$.numericResponse') AS int64) AS numeric_response,
                JSON_VALUE(value, '$.textResponse') AS text_response
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.values")) value 
        ) AS values
    FROM {{ source('staging', 'base_edfi_survey_question_responses') }}
    WHERE date_extracted >= (
        SELECT MAX(date_extracted) AS date_extracted
        FROM {{ source('staging', 'base_edfi_survey_question_responses') }}
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
