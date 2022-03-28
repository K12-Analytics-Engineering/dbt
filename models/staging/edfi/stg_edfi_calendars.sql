
WITH latest_extract AS (

    SELECT
        school_year,
        MAX(date_extracted) AS date_extracted
    FROM {{ source('staging', 'base_edfi_calendars') }}
    WHERE is_complete_extract IS TRUE
    GROUP BY 1

),

records AS (

    SELECT base_table.*
    FROM {{ source('staging', 'base_edfi_calendars') }} base_table
    LEFT JOIN latest_extract ON base_table.school_year = latest_extract.school_year
    WHERE
        base_table.date_extracted >= latest_extract.date_extracted
        AND id IS NOT NULL

)


SELECT
    date_extracted                          AS date_extracted,
    school_year                             AS school_year,
    id                                      AS id,
    JSON_VALUE(data, '$.calendarCode') AS calendar_code,
    STRUCT(
        JSON_VALUE(data, '$.schoolReference.schoolId') AS school_id
    ) AS school_reference,
    STRUCT(
        CAST(JSON_VALUE(data, '$.schoolYearTypeReference.schoolYear') AS int64) AS school_year
    ) AS school_year_type_reference,
    SPLIT(JSON_VALUE(data, "$.calendarTypeDescriptor"), '#')[OFFSET(1)] AS calendar_type_descriptor,
    ARRAY(
        SELECT AS STRUCT 
            SPLIT(JSON_VALUE(grade_levels, "$.gradeLevelDescriptor"), '#')[OFFSET(1)] AS grade_level_descriptor
        FROM UNNEST(JSON_QUERY_ARRAY(data, "$.gradeLevels")) grade_levels 
    ) AS grade_levels
FROM records
WHERE
    extract_type = 'records'
    AND id NOT IN (SELECT id FROM records WHERE extract_type = 'deletes') 
QUALIFY ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY date_extracted DESC) = 1
