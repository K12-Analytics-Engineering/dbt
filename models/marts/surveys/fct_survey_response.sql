{# 
select
    {{ dbt_utils.surrogate_key([
        'survey_question_responses.survey_question_reference.namespace',
        'survey_question_responses.survey_question_reference.survey_identifier'
    ]) }}                                               as survey_key,
    {{ dbt_utils.surrogate_key([
        'survey_question_responses.survey_question_reference.namespace',
        'survey_question_responses.survey_question_reference.survey_identifier',
        'survey_question_responses.survey_question_reference.question_code'
    ]) }}                                               as survey_question_key,
    {{ dbt_utils.surrogate_key([
        'survey_responses.student_reference.student_unique_id',
        'survey_question_responses.school_year'
    ]) }}                                               as student_key,
    survey_responses.survey_response_identifier         as survey_response_identifier,
    survey_responses.response_date                      as response_date,
    {# value.survey_question_response_value_identifier     as , #}
    value.numeric_response                              as numeric_response,
    value.text_response                                 as text_response
from {{ ref('stg_edfi_survey_question_responses') }} survey_question_responses
cross join unnest(survey_question_responses.values) as value
left join {{ ref('stg_edfi_survey_responses') }} survey_responses
    on survey_question_responses.survey_response_reference.namespace = survey_responses.survey_reference.namespace
    and survey_question_responses.survey_response_reference.survey_identifier = survey_responses.survey_reference.survey_identifier
    and survey_question_responses.survey_response_reference.survey_response_identifier = survey_responses.survey_response_identifier #}
