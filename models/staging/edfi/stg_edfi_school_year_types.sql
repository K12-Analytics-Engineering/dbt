
SELECT DISTINCT
    CAST(JSON_VALUE(data, '$.schoolYear') AS int64) school_year,
    JSON_VALUE(data, '$.schoolYearDescription') AS school_year_description
FROM {{ source('staging', 'base_edfi_school_year_types') }}
