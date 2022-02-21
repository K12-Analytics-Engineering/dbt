
SELECT
    STRUCT(
        'uri://forms.google.com'       AS namespace,
        response.question_id           AS questionCode,
        google_forms_responses.form_id AS surveyIdentifier
    )                                                       AS surveyQuestionReference,
    STRUCT(
        'uri://forms.google.com'                AS namespace,
        google_forms_responses.form_id          AS surveyIdentifier,
        google_forms_responses.response_id      AS surveyResponseIdentifier
    )                                                       AS surveyResponseReference,
    ARRAY(
        SELECT AS STRUCT
            1                           AS surveyQuestionResponseValueIdentifier,
            response.question_response  AS textResponse
    )                                                       AS values
FROM {{ ref('stg_google_forms_responses') }} AS google_forms_responses,
    UNNEST(google_forms_responses.responses) response
