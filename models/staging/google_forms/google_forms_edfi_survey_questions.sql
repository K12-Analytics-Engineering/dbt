
SELECT
    question_id                             AS questionCode,
    STRUCT(
        'uri://forms.google.com' AS namespace,
        form_id                  AS surveyIdentifier
    )                                       AS surveyReference,
    question_title                          AS questionText,
    CASE question_type
        WHEN "RADIO"    THEN "uri://ed-fi.org/QuestionFormDescriptor#Radio box"
        WHEN "CHECKBOX" THEN "uri://ed-fi.org/QuestionFormDescriptor#Checkbox"
        WHEN "TEXTBOX" THEN "uri://ed-fi.org/QuestionFormDescriptor#Textbox"
    END                                     AS questionFormDescriptor,
    ARRAY(
        SELECT AS STRUCT
            index + 1       AS sortOrder,
            ''              AS numericValue,
            values          AS textValue
        FROM UNNEST(question_values) AS values WITH OFFSET AS index
        WHERE values != ''
    )                                       AS response_choice
FROM {{ ref('stg_google_forms_questions') }}
WHERE question_type != 'LINEARSCALE'

UNION ALL

SELECT
    question_id                             AS questionCode,
    STRUCT(
        'uri://forms.google.com' AS namespace,
        form_id                  AS surveyIdentifier
    )                                       AS surveyReference,
    question_title                          AS questionText,
    'uri://forms.google.com/QuestionFormDescriptor#Linear Scale' AS questionFormDescriptor,
    ARRAY(
        SELECT AS STRUCT
            index + 1       AS sortOrder,
            values          AS numericValue,
            ''              AS textValue
        FROM UNNEST(question_values) AS values WITH OFFSET AS index
    )                                       AS response_choice
FROM {{ ref('stg_google_forms_questions') }}
WHERE question_type = 'LINEARSCALE'