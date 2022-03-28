
WITH records AS (

    SELECT *
    FROM {{ source('staging', 'base_edfi_calendar_dates') }}
    WHERE date_extracted >= (
        SELECT MAX(date_extracted) AS date_extracted
        FROM {{ source('staging', 'base_edfi_calendar_dates') }}
        WHERE is_complete_extract IS TRUE)

)

SELECT
    date_extracted                          AS date_extracted,
    school_year                             AS school_year,
    id                                      AS id,
    PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, '$.date')) AS date,
    ARRAY(
        SELECT AS STRUCT 
            SPLIT(JSON_VALUE(calendar_events, "$.calendarEventDescriptor"), '#')[OFFSET(1)] AS calendar_event_descriptor
        FROM UNNEST(JSON_QUERY_ARRAY(data, "$.calendarEvents")) calendar_events 
    ) AS calendar_events,
    STRUCT(
        JSON_VALUE(data, '$.calendarReference.calendarCode') AS calendar_code,
        JSON_VALUE(data, '$.calendarReference.schoolId') AS school_id,
        CAST(JSON_VALUE(data, '$.calendarReference.schoolYear') AS int64) AS school_year
    ) AS calendar_reference
FROM records
WHERE
    extract_type = 'records'
    AND id NOT IN (SELECT id FROM records WHERE extract_type = 'deletes') 
QUALIFY ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY date_extracted DESC) = 1
