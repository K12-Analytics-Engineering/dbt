
select
    struct(
        'uri://forms.google.com'       as namespace,
        response.question_id           as questionCode,
        google_forms_responses.form_id as surveyIdentifier
    )                                                       as surveyQuestionReference,
    struct(
        'uri://forms.google.com'                as namespace,
        google_forms_responses.form_id          as surveyIdentifier,
        google_forms_responses.response_id      as surveyResponseIdentifier
    )                                                       as surveyResponseReference,
    array(
        select as struct
            1                           as surveyQuestionResponseValueIdentifier,
            response.question_response  as textResponse
    )                                                       as values
from {{ ref('stg_google_forms_responses') }} as google_forms_responses,
    unnest(google_forms_responses.responses) response
