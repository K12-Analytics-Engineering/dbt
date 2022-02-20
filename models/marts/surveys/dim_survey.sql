
SELECT
    {{ dbt_utils.surrogate_key([
        'namespace',
        'survey_identifier'
    ]) }}                                       AS survey_key,
    school_year_type_reference.school_year      AS school_year,
    namespace                                   AS namespace,
    survey_identifier                           AS survey_identifier,
    survey_title                                AS title
FROM {{ ref('stg_edfi_surveys') }}
