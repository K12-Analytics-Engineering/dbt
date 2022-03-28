
{{ retrieve_edfi_records_from_data_lake('base_edfi_sessions') }}

SELECT
    date_extracted                          AS date_extracted,
    school_year                             AS school_year,
    id                                      AS id,
    JSON_VALUE(data, '$.sessionName') AS session_name,
    STRUCT(
        JSON_VALUE(data, '$.schoolReference.schoolId') AS school_id
    ) AS school_reference,
    STRUCT(
        CAST(JSON_VALUE(data, '$.schoolYearTypeReference.schoolYear') AS int64) AS school_year
    ) AS school_year_type_reference,
    SPLIT(JSON_VALUE(data, "$.termDescriptor"), '#')[OFFSET(1)] AS term_descriptor,
    CAST(JSON_VALUE(data, "$.totalInstructionalDays") AS int64) AS total_instructional_days,
    PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, "$.beginDate")) AS begin_date,
    PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, "$.endDate")) AS end_date,
    ARRAY(
        SELECT AS STRUCT
            STRUCT(
                SPLIT(JSON_VALUE(grading_periods, '$.gradingPeriodReference.gradingPeriodDescriptor'), '#')[OFFSET(1)] AS grading_period_descriptor,
                CAST(JSON_VALUE(grading_periods, "$.gradingPeriodReference.periodSequence") AS int64) AS period_sequence,
                JSON_VALUE(grading_periods, "$.gradingPeriodReference.schoolId") AS school_id,
                JSON_VALUE(grading_periods, "$.gradingPeriodReference.schoolYear") AS school_year
            ) AS grading_period_reference
        FROM UNNEST(JSON_QUERY_ARRAY(data, "$.gradingPeriods")) grading_periods 
    ) AS grading_periods
FROM records
WHERE
    extract_type = 'records'
    AND id NOT IN (SELECT id FROM records WHERE extract_type = 'deletes') 
QUALIFY ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY date_extracted DESC) = 1
