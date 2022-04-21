select NULL as column1
{# 
with parsed_data as (

    select
        date_extracted                          as date_extracted,
        school_year                             as school_year,
        json_value(data, '$.id') as id,
        json_value(data, '$.surveyResponseIdentifier') as survey_response_identifier,
        struct(
            json_value(data, '$.parentReference.parentUniqueId') as parent_unique_id
        ) as parent_reference,
        struct(
            json_value(data, '$.staffReference.staffUniqueId') as staff_unique_id
        ) as staff_reference,
        struct(
            json_value(data, '$.studentReference.studentUniqueId') as student_unique_id
        ) as student_reference,
        struct(
            json_value(data, '$.surveyReference.namespace') as namespace,
            json_value(data, '$.surveyReference.surveyIdentifier') as survey_identifier
        ) as survey_reference,
        json_value(data, '$.electronicMailAddress') as electronic_mail_address,
        json_value(data, '$.fullName') as full_name,
        json_value(data, '$.location') as location,
        parse_date('%Y-%m-%d', json_value(data, "$.responseDate")) as response_date,
        cast(json_value(data, "$.responseTime") as int64) as response_time,
        array(
            select as struct 
                split(json_value(survey_levels, '$.surveyLevelDescriptor'), '#')[OFFSET(1)] as survey_level_descriptor
            from unnest(json_query_array(data, "$.surveyLevels")) survey_levels 
        ) as survey_levels
    from {{ source('staging', 'base_edfi_survey_responses') }}
    where date_extracted >= (
        select max(date_extracted) as date_extracted
        from {{ source('staging', 'base_edfi_survey_responses') }}
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
