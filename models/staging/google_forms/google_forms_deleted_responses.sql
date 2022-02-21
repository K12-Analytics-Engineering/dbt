

SELECT
    edfi_survey_responses.id            AS survey_responses_id,
    edfi_survey_question_responses.id   AS survey_question_responses_id
FROM {{ ref('stg_edfi_survey_question_responses') }} AS edfi_survey_question_responses
LEFT JOIN {{ ref('stg_edfi_survey_responses') }} AS edfi_survey_responses
    ON edfi_survey_question_responses.survey_response_reference.namespace = edfi_survey_responses.survey_reference.namespace
    AND edfi_survey_question_responses.survey_response_reference.survey_identifier = edfi_survey_responses.survey_reference.survey_identifier
    AND edfi_survey_question_responses.survey_response_reference.survey_response_identifier = edfi_survey_responses.survey_response_identifier
LEFT JOIN {{ ref('google_forms_edfi_survey_question_responses') }} AS forms_survey_question_responses
    ON edfi_survey_question_responses.survey_response_reference.namespace = forms_survey_question_responses.surveyResponseReference.namespace
    AND edfi_survey_question_responses.survey_response_reference.survey_identifier = forms_survey_question_responses.surveyResponseReference.surveyIdentifier
    AND edfi_survey_question_responses.survey_response_reference.survey_response_identifier = forms_survey_question_responses.surveyResponseReference.surveyResponseIdentifier
WHERE forms_survey_question_responses.surveyResponseReference.surveyResponseIdentifier IS NULL

