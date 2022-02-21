
SELECT
    JSON_VALUE(data, '$.formId')                AS form_id,
    JSON_VALUE(data, '$.info.documentTitle')    AS form_title,
    JSON_VALUE(data, '$.revisionId')            AS revision_id
FROM {{ source('staging', 'base_google_forms_questions') }}
