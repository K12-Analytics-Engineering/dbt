

with dates as (
    select distinct
        date,
        calendar_reference.school_year
    from  {{ ref('stg_edfi_calendar_dates') }}
)


select
    date                                                    as date,
    extract(DAY from date)                                  as day,
    extract(month from date)                                as month,
    format_datetime('%B', date)                             as month_name,
    extract(quarter from date)                              as calendar_quarter,
    case
        when extract(quarter from date) = 1 then 'First'
        when extract(quarter from date) = 2 then 'Second'
        when extract(quarter from date) = 3 then 'Third'
        when extract(quarter from date) = 4 then 'Fourth'
    end                                                     as calendar_quarter_name,
    cast(school_year as int64)                              as calendar_year,
    if(
        extract(month from date) >= 7, 
        extract(month from date) - 6,
        extract(month from date) + 6
    )                                                       as month_sort_order
from dates
