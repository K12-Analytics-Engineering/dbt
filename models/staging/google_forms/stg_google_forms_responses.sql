
{% call set_sql_header(config) %}

    CREATE TEMP FUNCTION jsonObjectKeys(input STRING)
    RETURNS Array<struct<question_id STRING, question_response STRING>>
    LANGUAGE js as """

        var values = [];
        var object = JSON.parse(input);

        for (let key in object) {
            answers = object[key]['textAnswers']['answers'];
            for (let answer of answers) {
                values.push({
                    question_id: object[key]['questionId'],
                    question_response: answer['value']
                });
            }
        
        }
        
        return values;

    """;

{% endcall %}

with responses as (
    select  
        json_value(data, '$.formId')            as form_id,
        json_value(data, '$.responseId')        as response_id,
        json_value(data, '$.respondentEmail')   as respondent_email,
        question,
        cast(json_value(data, '$.lastSubmittedTime') as TIMESTAMP) as last_submitted
    from {{ source('staging', 'base_google_forms_responses') }}
    cross join unnest(jsonObjectKeys(JSON_QUERY(data, '$.answers'))) question
)

select
    form_id                     as form_id,
    to_hex(md5(response_id))    as response_id,
    respondent_email            as respondent_email,
    last_submitted              as last_submitted,
    ARRAY_AGG(
        struct(
            question.question_id,
            question.question_response
        )
    )                          as responses
from responses
group by
    form_id,
    response_id,
    respondent_email,
    last_submitted
