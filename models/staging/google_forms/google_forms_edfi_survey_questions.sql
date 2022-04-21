
select
    question_id                             as questionCode,
    struct(
        'uri://forms.google.com' as namespace,
        form_id                  as surveyIdentifier
    )                                       as surveyReference,
    question_title                          as questionText,
    case question_type
        when "RADIO"    then "uri://ed-fi.org/QuestionFormDescriptor#Radio box"
        when "CHECKBOX" then "uri://ed-fi.org/QuestionFormDescriptor#Checkbox"
        when "TEXTBOX" then "uri://ed-fi.org/QuestionFormDescriptor#Textbox"
    end                                     as questionFormDescriptor,
    array(
        select as struct
            index + 1       as sortOrder,
            ''              as numericValue,
            values          as textValue
        from unnest(question_values) as values with OFFSET as index
        where values != ''
    )                                       as response_choice
from {{ ref('stg_google_forms_questions') }}
where question_type != 'LINEARSCALE'

union all

select
    question_id                             as questionCode,
    struct(
        'uri://forms.google.com' as namespace,
        form_id                  as surveyIdentifier
    )                                       as surveyReference,
    question_title                          as questionText,
    'uri://forms.google.com/QuestionFormDescriptor#Linear Scale' as questionFormDescriptor,
    array(
        select as struct
            index + 1       as sortOrder,
            values          as numericValue,
            ''              as textValue
        from unnest(question_values) as values with OFFSET as index
    )                                       as response_choice
from {{ ref('stg_google_forms_questions') }}
where question_type = 'LINEARSCALE'