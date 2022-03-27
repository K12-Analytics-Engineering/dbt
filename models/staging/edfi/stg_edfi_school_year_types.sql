
SELECT DISTINCT
    date_extracted                                  AS date_extracted,
    CAST(JSON_VALUE(data, '$.schoolYear') AS int64) AS school_year,
    JSON_VALUE(data, '$.id')                        AS id,
    JSON_VALUE(data, '$.schoolYearDescription')     AS school_year_description
FROM {{ source('staging', 'base_edfi_school_year_types') }}
WHERE date_extracted >= (
    SELECT MAX(date_extracted) AS date_extracted
    FROM {{ source('staging', 'base_edfi_school_year_types') }}
    WHERE is_complete_extract IS TRUE)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY id
    ORDER BY date_extracted DESC) = 1
