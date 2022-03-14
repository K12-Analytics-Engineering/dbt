
WITH parsed_data AS (

    SELECT
        JSON_VALUE(data, '$.extractedTimestamp') AS extracted_timestamp,
        JSON_VALUE(data, '$.id') AS id,
        CAST(JSON_VALUE(data, '$.schoolYear') AS int64) school_year,
        STRUCT(
            JSON_VALUE(data, '$.schoolReference.schoolId') AS school_id
        ) AS school_reference,
        STRUCT(
            JSON_VALUE(data, '$.studentReference.studentUniqueId') AS student_unique_id
        ) AS student_reference,
        STRUCT(
            CAST(JSON_VALUE(data, '$.schoolYearTypeReference.schoolYear') AS int64) AS school_year
        ) AS school_year_type_reference,
        SPLIT(JSON_VALUE(data, '$.entryTypeDescriptor'), '#')[OFFSET(1)] AS entry_type_descriptor,
        SPLIT(JSON_VALUE(data, '$.entryGradeLevelDescriptor'), '#')[OFFSET(1)] AS entry_grade_level_descriptor,
        PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, '$.entryDate')) AS entry_date,
        PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, '$.exitWithdrawDate')) AS exit_withdraw_date,
        SPLIT(JSON_VALUE(data, '$.exitWithdrawTypeDescriptor'), '#')[OFFSET(1)] AS exit_withdraw_type_descriptor,
        CAST(JSON_VALUE(data, '$.fullTimeEquivalency') AS int64) AS full_time_equivalency,
        CAST(JSON_VALUE(data, '$.primarySchool') AS BOOL) AS primary_school,
        CAST(JSON_VALUE(data, '$.repeatGradeIndicator') AS BOOL) AS repeat_grade_indicator,
        CAST(JSON_VALUE(data, '$.schoolChoiceTransfer') AS BOOL) AS school_choice_transfer,
        CAST(JSON_VALUE(data, '$.termCompletionIndicator') AS BOOL) AS term_completion_indicator
    FROM {{ source('staging', 'base_edfi_student_school_associations') }}
    QUALIFY ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY extracted_timestamp DESC) = 1

)


SELECT *
FROM parsed_data
WHERE
    id NOT IN (
        SELECT id FROM {{ ref('stg_edfi_deletes') }} edfi_deletes
        WHERE parsed_data.school_year = edfi_deletes.school_year)
