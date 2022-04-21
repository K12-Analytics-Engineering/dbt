
with forms as (

    select
        json_value(data, '$.formId')                as form_id,
        struct(
            json_value(data, '$.info.documentTitle') as document_title
        )                                           as info,
        json_value(data, '$.info.documentTitle')    as form_title,
        json_value(data, '$.revisionId')            as revision_id,
        json_query_array(data, '$.items') as items
    from {{ source('staging', 'base_google_forms_questions') }}

),

forms_questions as (

    select 
        form_id,
        json_value(items, '$.title') as question_title,
        JSON_QUERY(items, '$.questionItem') as question_items,
    from forms
    cross join unnest(forms.items) as items
    where JSON_QUERY(items, '$.questionItem') is not null

),

questions_with_values as (

    select
        form_id,
        question_title,
        json_value(question_items, '$.question.choiceQuestion.type') as question_type,
        json_value(question_items, '$.question.questionId')          as question_id,
        json_value(question_values, '$.value')                       as question_value
    from forms_questions
    cross join unnest(json_query_array(question_items, '$.question.choiceQuestion.options')) question_values

    union all

    select
        form_id,
        question_title,
        'TEXTBOX'                                                   as question_type,
        json_value(question_items, '$.question.questionId')         as question_id,
        ''                                                          as question_value
    from forms_questions
    where REGEXP_CONTAINS(question_items, '"textQuestion"')

    union all 

    select
        form_id,
        question_title,
        'LINEARSCALE' as question_type,
        json_value(question_items, '$.question.questionId') as question_id,
        cast(question_value as STRING) as question_value
    from forms_questions
    cross join unnest(
        GENERATE_ARRAY(
            cast(json_value(question_items, '$.question.scaleQuestion.low') as int64),
            cast(json_value(question_items, '$.question.scaleQuestion.high') as int64)
        )
    ) as question_value
    where REGEXP_CONTAINS(question_items, '"scaleQuestion"')

)

select
    form_id,
    question_id,
    question_title,
    question_type,
    ARRAY_AGG(question_value) as question_values
from questions_with_values
group by
    form_id,
    question_id,
    question_title,
    question_type
