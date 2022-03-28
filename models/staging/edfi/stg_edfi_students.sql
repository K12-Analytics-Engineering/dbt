
WITH latest_extract AS (

    SELECT
        school_year,
        MAX(date_extracted) AS date_extracted
    FROM {{ source('staging', 'base_edfi_students') }}
    WHERE is_complete_extract IS TRUE
    GROUP BY 1

),

records AS (

    SELECT base_table.*
    FROM {{ source('staging', 'base_edfi_students') }} base_table
    LEFT JOIN latest_extract ON base_table.school_year = latest_extract.school_year
    WHERE
        base_table.date_extracted >= latest_extract.date_extracted
        AND id IS NOT NULL

)


SELECT
    date_extracted                          AS date_extracted,
    school_year                             AS school_year,
    id                                      AS id,
    JSON_VALUE(data, '$.studentUniqueId') AS student_unique_id,
    JSON_VALUE(data, '$.lastSurname') AS last_surname,
    JSON_VALUE(data, '$.middleName') AS middle_name,
    JSON_VALUE(data, '$.firstName') AS first_name,
    JSON_VALUE(data, '$.generationCodeSuffix') AS generation_code_suffix,
    PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, '$.birthDate')) AS birth_date,
    JSON_VALUE(data, '$.birthCity') AS birth_city,
    SPLIT(JSON_VALUE(data, "$.birthCountryDescriptor"), '#')[OFFSET(1)] AS birth_country_descriptor,
    JSON_VALUE(data, '$.birthInternationalProvince') AS birth_international_province,
    STRUCT(
        JSON_VALUE(data, '$.personReference.personId') AS person_id,
        SPLIT(JSON_VALUE(data, "$.personReference.sourceSystemDescriptor"), '#')[OFFSET(1)] AS source_system_descriptor
    ) AS person_reference,
FROM records
WHERE
    extract_type = 'records'
    AND id NOT IN (SELECT id FROM records WHERE extract_type = 'deletes') 
QUALIFY ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY date_extracted DESC) = 1
