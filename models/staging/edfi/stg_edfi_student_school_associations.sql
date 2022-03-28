
WITH latest_extract AS (

    SELECT
        school_year,
        MAX(date_extracted) AS date_extracted
    FROM {{ source('staging', 'base_edfi_student_school_associations') }}
    WHERE is_complete_extract IS TRUE
    GROUP BY 1

),

records AS (

    SELECT base_table.*
    FROM {{ source('staging', 'base_edfi_student_school_associations') }} base_table
    LEFT JOIN latest_extract ON base_table.school_year = latest_extract.school_year
    WHERE
        base_table.date_extracted >= latest_extract.date_extracted
        AND id IS NOT NULL

)


SELECT
    date_extracted                          AS date_extracted,
    school_year                             AS school_year,
    JSON_VALUE(data, '$.id') AS id,
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
FROM records
WHERE
    extract_type = 'records'
    AND id NOT IN (SELECT id FROM records WHERE extract_type = 'deletes') 
QUALIFY ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY date_extracted DESC) = 1
