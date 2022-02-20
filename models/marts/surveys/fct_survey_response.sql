
SELECT
    {{ dbt_utils.surrogate_key([
        'survey_question_responses.survey_question_reference.namespace',
        'survey_question_responses.survey_question_reference.survey_identifier'
    ]) }}                                               AS survey_key,
    {{ dbt_utils.surrogate_key([
        'survey_question_responses.survey_question_reference.namespace',
        'survey_question_responses.survey_question_reference.survey_identifier',
        'survey_question_responses.survey_question_reference.question_code'
    ]) }}                                               AS survey_question_key,
    {{ dbt_utils.surrogate_key([
        'survey_responses.student_reference.student_unique_id',
        'survey_question_responses.school_year'
    ]) }}                                               AS student_key,
    survey_responses.survey_response_identifier         AS survey_response_identifier,
    survey_responses.response_date                      AS response_date,
    {# value.survey_question_response_value_identifier     AS , #}
    value.numeric_response                              AS numeric_response,
    value.text_response                                 AS text_response
FROM {{ ref('stg_edfi_survey_question_responses') }} survey_question_responses
CROSS JOIN UNNEST(survey_question_responses.values) AS value
LEFT JOIN {{ ref('stg_edfi_survey_responses') }} survey_responses
    ON survey_question_responses.survey_response_reference.namespace = survey_responses.survey_reference.namespace
    AND survey_question_responses.survey_response_reference.survey_identifier = survey_responses.survey_reference.survey_identifier
    AND survey_question_responses.survey_response_reference.survey_response_identifier = survey_responses.survey_response_identifier
