select NULL as column1
{# 
with parsed_data as (

    select
        date_extracted                          as date_extracted,
        school_year                             as school_year,
        json_value(data, '$.id') as id,
        json_value(data, '$.namespace') as namespace,
        json_value(data, '$.surveyIdentifier') as survey_identifier,
        json_value(data, '$.surveyTitle') as survey_title,
        struct(
            json_value(data, '$.educationOrganizationReference.educationOrganizationId') as education_organization_id
        ) as education_organization_reference,
        struct(
            cast(json_value(data, '$.schoolYearTypeReference.schoolYear') as int64) as school_year
        ) as school_year_type_reference,
        struct(
            json_value(data, '$.sessionReference.schoolId') as school_id,
            cast(json_value(data, '$.sessionReference.schoolYear') as int64) as school_year,
            json_value(data, '$.sessionReference.sessionName') as session_name
        ) as session_reference,
        cast(json_value(data, '$.numberAdministered') as int64) number_administered,
        split(json_value(data, '$.surveyCategoryDescriptor'), '#')[OFFSET(1)] as survey_category_descriptor
    from {{ source('staging', 'base_edfi_surveys') }}
    where date_extracted >= (
        select max(date_extracted) as date_extracted
        from {{ source('staging', 'base_edfi_surveys') }}
        where is_complete_extract is true)
    qualify row_number() over (
            partition by id
            order by date_extracted DESC) = 1

)


select *
from parsed_data
where
    id not in (
        select id from {{ ref('stg_edfi_deletes') }} edfi_deletes
        where parsed_data.school_year = edfi_deletes.school_year) #}
