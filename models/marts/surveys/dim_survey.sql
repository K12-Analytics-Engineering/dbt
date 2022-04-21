{# 
select
    {{ dbt_utils.surrogate_key([
        'namespace',
        'survey_identifier'
    ]) }}                                       as survey_key,
    school_year_type_reference.school_year      as school_year,
    namespace                                   as namespace,
    survey_identifier                           as survey_identifier,
    survey_title                                as title
from {{ ref('stg_edfi_surveys') }} #}
