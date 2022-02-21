
WITH forms AS (

    SELECT
        JSON_VALUE(data, '$.formId')                AS form_id,
        STRUCT(
            JSON_VALUE(data, '$.info.documentTitle') AS document_title
        )                                           AS info,
        JSON_VALUE(data, '$.info.documentTitle')    AS form_title,
        JSON_VALUE(data, '$.revisionId')            AS revision_id,
        JSON_QUERY_ARRAY(data, '$.items') AS items
    FROM {{ source('staging', 'base_google_forms_questions') }}

),

forms_questions AS (

    SELECT 
        form_id,
        JSON_VALUE(items, '$.title') AS question_title,
        JSON_QUERY(items, '$.questionItem') AS question_items,
    FROM forms
    CROSS JOIN UNNEST(forms.items) as items
    WHERE JSON_QUERY(items, '$.questionItem') IS NOT NULL

),

questions_with_values AS (

    SELECT
        form_id,
        question_title,
        JSON_VALUE(question_items, '$.question.choiceQuestion.type') AS question_type,
        JSON_VALUE(question_items, '$.question.questionId')          AS question_id,
        JSON_VALUE(question_values, '$.value')                       AS question_value
    FROM forms_questions
    CROSS JOIN UNNEST(JSON_QUERY_ARRAY(question_items, '$.question.choiceQuestion.options')) question_values

    UNION ALL

    SELECT
        form_id,
        question_title,
        'TEXTBOX'                                                   AS question_type,
        JSON_VALUE(question_items, '$.question.questionId')         AS question_id,
        ''                                                          AS question_value
    FROM forms_questions
    WHERE REGEXP_CONTAINS(question_items, '"textQuestion"')

    UNION ALL 

    SELECT
        form_id,
        question_title,
        'LINEARSCALE' AS question_type,
        JSON_VALUE(question_items, '$.question.questionId') AS question_id,
        CAST(question_value AS STRING) AS question_value
    FROM forms_questions
    CROSS JOIN UNNEST(
        GENERATE_ARRAY(
            CAST(JSON_VALUE(question_items, '$.question.scaleQuestion.low') AS int64),
            CAST(JSON_VALUE(question_items, '$.question.scaleQuestion.high') AS int64)
        )
    ) AS question_value
    WHERE REGEXP_CONTAINS(question_items, '"scaleQuestion"')

)

SELECT
    form_id,
    question_id,
    question_title,
    question_type,
    ARRAY_AGG(question_value) AS question_values
FROM questions_with_values
GROUP BY
    form_id,
    question_id,
    question_title,
    question_type
