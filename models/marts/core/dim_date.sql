{{
  config(
    labels = {'analytics_middle_tier': 'yes'}
  )
}}


WITH dates AS (
    SELECT DISTINCT
        date,
        calendar_reference.school_year
    FROM  {{ ref('stg_edfi_calendar_dates') }}
)


SELECT
    date                                                    AS date,
    EXTRACT(DAY FROM date)                                  AS day,
    EXTRACT(MONTH FROM date)                                AS month,
    FORMAT_DATETIME('%B', date)                             AS month_name,
    EXTRACT(QUARTER FROM date)                              AS calendar_quarter,
    CASE
        WHEN EXTRACT(QUARTER FROM date) = 1 THEN 'First'
        WHEN EXTRACT(QUARTER FROM date) = 2 THEN 'Second'
        WHEN EXTRACT(QUARTER FROM date) = 3 THEN 'Third'
        WHEN EXTRACT(QUARTER FROM date) = 4 THEN 'Fourth'
    END                                                     AS calendar_quarter_name,
    CAST(school_year AS int64)                              AS calendar_year,
    IF(
        EXTRACT(MONTH FROM date) >= 7, 
        EXTRACT(MONTH FROM date) - 6,
        EXTRACT(MONTH FROM date) + 6
    )                                                       AS month_sort_order -- note in core amt
FROM dates
