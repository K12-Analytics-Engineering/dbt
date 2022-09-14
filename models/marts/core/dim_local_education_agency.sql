
select
    {{ dbt_utils.surrogate_key([
        'local_education_agency_id',
    ]) }}                               as local_education_agency_key,
    local_education_agency_id           as local_education_agency_id,
    name_of_institution                 as local_education_agency_name
from {{ ref('stg_edfi_local_education_agencies') }}
qualify rank() over (
    partition by local_education_agency_key
    order by school_year desc) = 1
