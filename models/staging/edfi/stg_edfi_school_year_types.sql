

WITH latest_extract AS (

    SELECT
        school_year,
        MAX(date_extracted) AS date_extracted
    FROM {{ source('staging', 'base_edfi_school_year_types') }}
    WHERE is_complete_extract IS TRUE
    GROUP BY 1

),

records AS (

    SELECT base_edfi_school_year_types.*
    FROM {{ source('staging', 'base_edfi_school_year_types') }} base_edfi_school_year_types
    LEFT JOIN latest_extract ON base_edfi_school_year_types.school_year = latest_extract.school_year
    WHERE
        base_edfi_school_year_types.date_extracted >= latest_extract.date_extracted
        AND id IS NOT NULL

)


SELECT DISTINCT
    date_extracted                                  AS date_extracted,
    CAST(JSON_VALUE(data, '$.schoolYear') AS int64) AS school_year,
    id                                      AS id,
    JSON_VALUE(data, '$.schoolYearDescription')     AS school_year_description
FROM records
WHERE
    extract_type = 'records'
    AND id NOT IN (SELECT id FROM records WHERE extract_type = 'deletes') 
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY id
    ORDER BY date_extracted DESC) = 1
