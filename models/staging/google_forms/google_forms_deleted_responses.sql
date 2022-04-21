

select
    edfi_survey_responses.id            as survey_responses_id,
    edfi_survey_question_responses.id   as survey_question_responses_id
from {{ ref('stg_edfi_survey_question_responses') }} as edfi_survey_question_responses
left join {{ ref('stg_edfi_survey_responses') }} as edfi_survey_responses
    on edfi_survey_question_responses.survey_response_reference.namespace = edfi_survey_responses.survey_reference.namespace
    and edfi_survey_question_responses.survey_response_reference.survey_identifier = edfi_survey_responses.survey_reference.survey_identifier
    and edfi_survey_question_responses.survey_response_reference.survey_response_identifier = edfi_survey_responses.survey_response_identifier
left join {{ ref('google_forms_edfi_survey_question_responses') }} as forms_survey_question_responses
    on edfi_survey_question_responses.survey_response_reference.namespace = forms_survey_question_responses.surveyResponseReference.namespace
    and edfi_survey_question_responses.survey_response_reference.survey_identifier = forms_survey_question_responses.surveyResponseReference.surveyIdentifier
    and edfi_survey_question_responses.survey_response_reference.survey_response_identifier = forms_survey_question_responses.surveyResponseReference.surveyResponseIdentifier
where forms_survey_question_responses.surveyResponseReference.surveyResponseIdentifier is null

