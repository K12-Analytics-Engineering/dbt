
{{ retrieve_edfi_records_from_data_lake('base_edfi_calendar_dates') }}

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

{{ remove_edfi_deletes_and_duplicates() }}
