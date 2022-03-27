
WITH parsed_data AS (

    SELECT
        date_extracted                          AS date_extracted,
        school_year                             AS school_year,
        JSON_VALUE(data, '$.id') AS id,
        SPLIT(JSON_VALUE(data, "$.gradingPeriodDescriptor"), '#')[OFFSET(1)] AS grading_period_descriptor,
        CAST(JSON_VALUE(data, "$.periodSequence") AS int64) AS period_sequence,
        STRUCT(
            JSON_VALUE(data, '$.schoolReference.schoolId') AS school_id
        ) AS school_reference,
        STRUCT(
            CAST(JSON_VALUE(data, '$.schoolYearTypeReference.schoolYear') AS int64) AS school_year
        ) AS school_year_type_reference,
        PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, "$.beginDate")) AS begin_date,
        PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, "$.endDate")) AS end_date,
        CAST(JSON_VALUE(data, "$.totalInstructionalDays") AS int64) AS total_instructional_days
    FROM {{ source('staging', 'base_edfi_grading_periods') }}
    WHERE date_extracted >= (
        SELECT MAX(date_extracted) AS date_extracted
        FROM {{ source('staging', 'base_edfi_grading_periods') }}
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
