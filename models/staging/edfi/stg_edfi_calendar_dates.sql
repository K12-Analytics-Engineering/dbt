
{{ retrieve_edfi_records_from_data_lake('base_edfi_calendar_dates') }}

select
    date_extracted                          as date_extracted,
    school_year                             as school_year,
    id                                      as id,
    parse_date('%Y-%m-%d', json_value(data, '$.date')) as date,
    array(
        select as struct 
            split(json_value(calendar_events, "$.calendarEventDescriptor"), '#')[OFFSET(1)] as calendar_event_descriptor
        from unnest(json_query_array(data, "$.calendarEvents")) calendar_events 
    ) as calendar_events,
    struct(
        json_value(data, '$.calendarReference.calendarCode') as calendar_code,
        json_value(data, '$.calendarReference.schoolId') as school_id,
        cast(json_value(data, '$.calendarReference.schoolYear') as int64) as school_year
    ) as calendar_reference
from records

{{ remove_edfi_deletes_and_duplicates() }}
