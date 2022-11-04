
select
    {{ dbt_utils.surrogate_key([
        'lea_id',
    ]) }}                               as lea_key,
    lea_id                              as lea_id,
    lea_name                            as lea_name,
    lea_category                        as lea_category,
    operational_status                  as operational_status
from {{ ref('stg_edfi_local_education_agencies') }}
qualify rank() over (
    partition by lea_key
    order by school_year desc) = 1
