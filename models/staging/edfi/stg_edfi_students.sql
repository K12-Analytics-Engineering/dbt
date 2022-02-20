
WITH parsed_data AS (

    SELECT
        JSON_VALUE(data, '$.extractedTimestamp') AS extracted_timestamp,
        JSON_VALUE(data, '$.id') AS id,
        CAST(JSON_VALUE(data, '$.schoolYear') AS int64) school_year,
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
    FROM {{ source('staging', 'base_edfi_students') }}

),

ranked AS (

    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY school_year, student_unique_id
            ORDER BY school_year DESC, extracted_timestamp DESC
        ) AS rank,
        *
    FROM parsed_data

)

SELECT * EXCEPT (extracted_timestamp, rank)
FROM ranked
WHERE
    rank = 1
    AND id NOT IN (
        SELECT id FROM {{ ref('stg_edfi_deletes') }} edfi_deletes
        WHERE ranked.school_year = edfi_deletes.school_year
    )
