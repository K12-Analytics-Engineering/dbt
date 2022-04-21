
select
    {{ dbt_utils.surrogate_key([
        'schools.school_id',
        'schools.school_year'
    ]) }}                                   as school_key,
    {{ dbt_utils.surrogate_key([
        'leas.local_education_agency_id',
        'leas.school_year'
    ]) }}                                   as local_education_agency_key,
    schools.school_year                     as school_year,
    schools.school_id                       as school_id,
    schools.name_of_institution             as school_name,
    schools.school_type_descriptor          as school_type,
    leas.name_of_institution                as local_education_agency_name
from {{ ref('stg_edfi_schools') }} schools
left join {{ ref('stg_edfi_local_education_agencies') }} leas
    on schools.school_year = leas.school_year
    and leas.local_education_agency_id = schools.local_education_agency_id
