
WITH latest_extract AS (

    SELECT
        school_year,
        MAX(date_extracted) AS date_extracted
    FROM {{ source('staging', 'base_edfi_descriptors') }}
    WHERE is_complete_extract IS TRUE
    GROUP BY 1

),

records AS (

    SELECT base_table.*
    FROM {{ source('staging', 'base_edfi_descriptors') }} base_table
    LEFT JOIN latest_extract ON base_table.school_year = latest_extract.school_year
    WHERE
        base_table.date_extracted >= latest_extract.date_extracted
        AND id IS NOT NULL

)


SELECT
    date_extracted                          AS date_extracted,
    school_year                             AS school_year,
    id                                      AS id,
    JSON_VALUE(data, '$.codeValue') AS code_value,
    JSON_VALUE(data, '$.description') AS description,
    JSON_VALUE(data, '$.namespace') AS namespace,
    JSON_VALUE(data, '$.shortDescription') AS short_description
FROM records
WHERE
    extract_type = 'records'
    AND id NOT IN (SELECT id FROM records WHERE extract_type = 'deletes') 
QUALIFY ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY date_extracted DESC) = 1
