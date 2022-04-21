select NULL as column1
{# 
with parsed_data as (

    select
        date_extracted                          as date_extracted,
        school_year                             as school_year,
        json_value(data, '$.id') as id,
        struct(
            json_value(data, '$.surveyQuestionReference.namespace') as namespace,
            json_value(data, '$.surveyQuestionReference.questionCode') as question_code,
            json_value(data, '$.surveyQuestionReference.surveyIdentifier') as survey_identifier
        ) as survey_question_reference,
        struct(
            json_value(data, '$.surveyResponseReference.namespace') as namespace,
            json_value(data, '$.surveyResponseReference.surveyIdentifier') as survey_identifier,
            json_value(data, '$.surveyResponseReference.surveyResponseIdentifier') as survey_response_identifier
        ) as survey_response_reference,
        json_value(data, '$.comment') as comment,
        cast(json_value(data, '$.noResponse') as BOOL) as no_response,
        array(
            select as struct
                json_value(matrix_response, '$.matrixElement') as matrix_element,
                json_value(matrix_response, '$.textResponse') as text_response,
                cast(json_value(matrix_response, '$.minNumericResponse') as int64) as min_numeric_response,
                cast(json_value(matrix_response, '$.maxNumericResponse') as int64) as max_numeric_response,
                cast(json_value(matrix_response, '$.numericResponse') as int64) as numeric_response,
                cast(json_value(matrix_response, '$.noResponse') as BOOL) as no_response
            from unnest(json_query_array(data, "$.surveyQuestionMatrixElementResponses")) matrix_response 
        ) as survey_question_matrix_element_responses,
        array(
            select as struct
                json_value(value, '$.surveyQuestionResponseValueIdentifier') as survey_question_response_value_identifier,
                cast(json_value(value, '$.numericResponse') as int64) as numeric_response,
                json_value(value, '$.textResponse') as text_response
            from unnest(json_query_array(data, "$.values")) value 
        ) as values
    from {{ source('staging', 'base_edfi_survey_question_responses') }}
    where date_extracted >= (
        select max(date_extracted) as date_extracted
        from {{ source('staging', 'base_edfi_survey_question_responses') }}
        where is_complete_extract is true)
    qualify row_number() over (
            partition by id
            order by date_extracted DESC) = 1

)


select *
from parsed_data
where
    id not in (
        select id from {{ ref('stg_edfi_deletes') }} edfi_deletes
        where parsed_data.school_year = edfi_deletes.school_year) #}
