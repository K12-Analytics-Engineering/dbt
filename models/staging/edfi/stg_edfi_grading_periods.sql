
{{ retrieve_edfi_records_from_data_lake('base_edfi_grading_periods') }}

SELECT
    date_extracted                          AS date_extracted,
    school_year                             AS school_year,
    id                                      AS id,
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
FROM records

{{ remove_edfi_deletes_and_duplicates() }}
