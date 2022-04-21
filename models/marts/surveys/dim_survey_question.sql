{# 
select
    {{ dbt_utils.surrogate_key([
        'survey_reference.namespace',
        'survey_reference.survey_identifier',
        'question_code'
    ]) }}                                       as survey_question_key,
    {{ dbt_utils.surrogate_key([
        'survey_reference.namespace',
        'survey_reference.survey_identifier'
    ]) }}                                       as survey_key,
    question_code                               as survey_question_identifier,
    question_text                               as text,
    question_form_descriptor                    as type
from {{ ref('stg_edfi_survey_questions') }} #}
