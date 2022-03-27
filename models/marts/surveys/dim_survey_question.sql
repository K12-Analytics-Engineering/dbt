{# 
SELECT
    {{ dbt_utils.surrogate_key([
        'survey_reference.namespace',
        'survey_reference.survey_identifier',
        'question_code'
    ]) }}                                       AS survey_question_key,
    {{ dbt_utils.surrogate_key([
        'survey_reference.namespace',
        'survey_reference.survey_identifier'
    ]) }}                                       AS survey_key,
    question_code                               AS survey_question_identifier,
    question_text                               AS text,
    question_form_descriptor                    AS type
FROM {{ ref('stg_edfi_survey_questions') }} #}
