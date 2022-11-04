
select
    {{ dbt_utils.surrogate_key([
        'schools.school_id',
        'schools.school_year'
    ]) }}                                   as school_key,
    leas.lea_name                           as lea_name,
    schools.school_id                       as school_id,
    schools.school_name                     as school_name,
    schools.school_short_name               as school_short_name,
    schools.school_type                     as school_type,
    array(select grade_level from unnest(schools.grade_levels)) as grade_levels
from {{ ref('stg_edfi_schools') }} schools
left join {{ ref('stg_edfi_local_education_agencies') }} leas
    on schools.school_year = leas.school_year
    and leas.lea_id = schools.lea_id
qualify rank() over (
    partition by school_key
    order by schools.school_year desc) = 1
