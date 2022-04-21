select NULL as column1
{# 
with parsed_data as (

    select
        date_extracted                          as date_extracted,
        school_year                             as school_year,
        json_value(data, '$.id') as id,
        json_value(data, '$.questionCode') as question_code,
        json_value(data, '$.questionText') as question_text,
        split(json_value(data, '$.questionFormDescriptor'), '#')[OFFSET(1)] as question_form_descriptor,
        array(
            select as struct 
                json_value(response_choices, "$.textValue") as text_value,
                cast(json_value(response_choices, "$.sortOrder") as int64) as sort_order,
                cast(json_value(response_choices, "$.numericValue") as int64) as numeric_value
            from unnest(json_query_array(data, "$.responseChoices")) response_choices 
        ) as response_choices,
        struct(
            json_value(data, '$.surveyReference.namespace') as namespace,
            json_value(data, '$.surveyReference.surveyIdentifier') as survey_identifier
        ) as survey_reference,
        struct(
            json_value(data, '$.surveySectionReference.namespace') as namespace,
            json_value(data, '$.surveySectionReference.surveyIdentifier') as survey_identifier,
            json_value(data, '$.surveySectionReference.surveySectionTitle') as survey_section_title
        ) as survey_section_reference,
        array(
            select as struct 
                json_value(matrices, "$.matrixElement") as matrix_element,
                cast(json_value(matrices, "$.minRawScore") as int64) as min_raw_score,
                cast(json_value(matrices, "$.maxRawScore") as int64) as max_raw_score
            from unnest(json_query_array(data, "$.matrices")) matrices 
        ) as matrices
    from {{ source('staging', 'base_edfi_survey_questions') }}
    where date_extracted >= (
        select max(date_extracted) as date_extracted
        from {{ source('staging', 'base_edfi_survey_questions') }}
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
