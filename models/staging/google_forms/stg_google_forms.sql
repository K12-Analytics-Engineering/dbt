
select
    json_value(data, '$.formId')                as form_id,
    json_value(data, '$.info.documentTitle')    as form_title,
    json_value(data, '$.revisionId')            as revision_id
from {{ source('staging', 'base_google_forms_questions') }}
