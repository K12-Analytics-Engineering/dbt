
select
    {{ dbt_utils.surrogate_key([
        'local_education_agency_id',
        'school_year'
    ]) }}                               as local_education_agency_key,
    school_year                         as school_year,
    local_education_agency_id           as local_education_agency_id,
    name_of_institution                 as local_education_agency_name
from {{ ref('stg_edfi_local_education_agencies') }}
