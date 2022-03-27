
WITH parsed_data AS (

    SELECT
        date_extracted                          AS date_extracted,
        school_year                             AS school_year,
        JSON_VALUE(data, '$.id') AS id,
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
    FROM {{ source('staging', 'base_edfi_calendars') }}
    WHERE date_extracted >= (
        SELECT MAX(date_extracted) AS date_extracted
        FROM {{ source('staging', 'base_edfi_calendars') }}
        WHERE is_complete_extract IS TRUE)
    QUALIFY ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY date_extracted DESC) = 1

)


SELECT * EXCEPT (school_year),
    COALESCE(school_year_type_reference.school_year, school_year) AS school_year
FROM parsed_data
WHERE
    id NOT IN (
        SELECT id FROM {{ ref('stg_edfi_deletes') }} edfi_deletes
        WHERE parsed_data.school_year = edfi_deletes.school_year)
