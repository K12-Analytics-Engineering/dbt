{{
  config(
    labels = {'analytics_middle_tier': 'yes'}
  )
}}


SELECT
    {{ dbt_utils.surrogate_key([
        'local_education_agency_id',
        'school_year'
    ]) }}                               AS local_education_agency_key,
    school_year                         AS school_year,
    local_education_agency_id           AS local_education_agency_id,
    name_of_institution                 AS local_education_agency_name
FROM {{ ref('stg_edfi_local_education_agencies') }}
