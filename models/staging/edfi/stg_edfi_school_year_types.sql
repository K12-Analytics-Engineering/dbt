

WITH records AS (

    SELECT *
    FROM {{ source('staging', 'base_edfi_school_year_types') }}
    WHERE date_extracted >= (
        SELECT MAX(date_extracted) AS date_extracted
        FROM {{ source('staging', 'base_edfi_school_year_types') }}
        WHERE is_complete_extract IS TRUE)

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
