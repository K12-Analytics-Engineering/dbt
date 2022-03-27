
WITH parsed_data AS (

    SELECT
        date_extracted                          AS date_extracted,
        school_year                             AS school_year,
        JSON_VALUE(data, '$.id') AS id,
        JSON_VALUE(data, '$.localEducationAgencyId') AS local_education_agency_id,
        JSON_VALUE(data, '$.nameOfInstitution') AS name_of_institution
    FROM {{ source('staging', 'base_edfi_local_education_agencies') }}
    WHERE date_extracted >= (
        SELECT MAX(date_extracted) AS date_extracted
        FROM {{ source('staging', 'base_edfi_local_education_agencies') }}
        WHERE is_complete_extract IS TRUE)
    QUALIFY ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY date_extracted DESC) = 1

)


SELECT *
FROM parsed_data
WHERE
    id NOT IN (
        SELECT id FROM {{ ref('stg_edfi_deletes') }} edfi_deletes
        WHERE parsed_data.school_year = edfi_deletes.school_year)
