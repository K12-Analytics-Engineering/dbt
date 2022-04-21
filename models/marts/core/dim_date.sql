

with dates as (
    select distinct
        date,
        calendar_reference.school_year
    from  {{ ref('stg_edfi_calendar_dates') }}
)


select
    date                                                    as date,
    EXTRACT(DAY from date)                                  as day,
    EXTRACT(MONTH from date)                                as month,
    FORMAT_DATETIME('%B', date)                             as month_name,
    EXTRACT(QUARTER from date)                              as calendar_quarter,
    case
        when EXTRACT(QUARTER from date) = 1 then 'First'
        when EXTRACT(QUARTER from date) = 2 then 'Second'
        when EXTRACT(QUARTER from date) = 3 then 'Third'
        when EXTRACT(QUARTER from date) = 4 then 'Fourth'
    end                                                     as calendar_quarter_name,
    cast(school_year as int64)                              as calendar_year,
    if(
        EXTRACT(MONTH from date) >= 7, 
        EXTRACT(MONTH from date) - 6,
        EXTRACT(MONTH from date) + 6
    )                                                       as month_sort_order -- note in core amt
from dates
