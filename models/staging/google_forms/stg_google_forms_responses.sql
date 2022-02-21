
{% call set_sql_header(config) %}

    CREATE TEMP FUNCTION jsonObjectKeys(input STRING)
    RETURNS Array<STRUCT<question_id STRING, question_response STRING>>
    LANGUAGE js AS """

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

WITH responses AS (
    SELECT  
        JSON_VALUE(data, '$.formId')            AS form_id,
        JSON_VALUE(data, '$.responseId')        AS response_id,
        JSON_VALUE(data, '$.respondentEmail')   AS respondent_email,
        question,
        CAST(JSON_VALUE(data, '$.lastSubmittedTime') AS TIMESTAMP) AS last_submitted
    FROM {{ source('staging', 'base_google_forms_responses') }}
    CROSS JOIN UNNEST(jsonObjectKeys(JSON_QUERY(data, '$.answers'))) question
)

SELECT
    form_id                     AS form_id,
    to_hex(md5(response_id))    AS response_id,
    respondent_email            AS respondent_email,
    last_submitted              AS last_submitted,
    ARRAY_AGG(
        STRUCT(
            question.question_id,
            question.question_response
        )
    )                          AS responses
FROM responses
GROUP BY
    form_id,
    response_id,
    respondent_email,
    last_submitted
