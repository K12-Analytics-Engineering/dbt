

WITH parsed_data AS (

    SELECT
        CAST(JSON_VALUE(data, '$.extractedTimestamp') AS TIMESTAMP) AS extracted_timestamp,
        JSON_VALUE(data, '$.id') AS id,
        CAST(JSON_VALUE(data, '$.schoolYear') AS int64) school_year,
        PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, "$.beginDate")) AS begin_date,
        PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, "$.endDate")) AS end_date,
        PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, "$.iepBeginDate")) AS iep_begin_date,
        PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, "$.iepEndDate")) AS iep_end_date,
        PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, "$.iepReviewDate")) AS iep_review_date,
        PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, "$.lastEvaluationDate")) AS last_evaluation_date,
        CAST(JSON_VALUE(data, '$.ideaEligibility') AS BOOL) idea_eligibility,
        CAST(JSON_VALUE(data, '$.medicallyFragile') AS BOOL) medically_fragile,
        CAST(JSON_VALUE(data, '$.multiplyDisabled') AS BOOL) multiply_disabled,
        CAST(JSON_VALUE(data, '$.servedOutsideOfRegularSession') AS BOOL) served_outside_of_regular_session,
        SPLIT(JSON_VALUE(data, "$.reasonExitedDescriptor"), '#')[OFFSET(1)] AS reason_exited_descriptor,
        SPLIT(JSON_VALUE(data, "$.specialEducationSettingDescriptor"), '#')[OFFSET(1)] AS special_education_setting_descriptor,
        CAST(JSON_VALUE(data, '$.schoolHoursPerWeek') AS float64) AS school_hours_per_week,
        CAST(JSON_VALUE(data, '$.specialEducationHoursPerWeek') AS float64) AS special_education_hours_per_week,
        STRUCT(
            JSON_VALUE(data, '$.educationOrganizationReference.educationOrganizationId') AS education_organization_id
        ) AS education_organization_reference,
        STRUCT(
            JSON_VALUE(data, '$.programReference.educationOrganizationId') AS education_organization_id,
            JSON_VALUE(data, '$.programReference.programName') AS program_name,
            SPLIT(JSON_VALUE(data, "$.programReference.programTypeDescriptor"), '#')[OFFSET(1)] AS program_type_descriptor
        ) AS program_reference,
        STRUCT(
            JSON_VALUE(data, '$.studentReference.studentUniqueId') AS student_unique_id
        ) AS student_reference,
        ARRAY(
            SELECT AS STRUCT 
                SPLIT(JSON_VALUE(disabilities, "$.disabilityDescriptor"), '#')[OFFSET(1)] AS disability_descriptor,
                SPLIT(JSON_VALUE(disabilities, "$.disabilityDeterminationSourceTypeDescriptor"), '#')[OFFSET(1)] AS disability_determination_source_type_descriptor,
                JSON_VALUE(disabilities, '$.disabilityDiagnosis') AS disability_diagnosis,
                CAST(JSON_VALUE(disabilities, '$.orderOfDisability') AS int64) AS order_of_disability
                -- designations array
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.disabilities")) disabilities 
        ) AS disabilities,
        STRUCT(
            SPLIT(JSON_VALUE(data, "$.participationStatusDescriptor"), '#')[OFFSET(1)] AS participation_status_descriptor,
            JSON_VALUE(data, '$.designatedBy') AS designated_by,
            PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, "$.statusBeginDate")) AS status_begin_date,
            PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, "$.statusEndDate")) AS status_end_date
        ) AS participation_status,
        ARRAY(
            SELECT AS STRUCT 
                SPLIT(JSON_VALUE(statuses, "$.participationStatusDescriptor"), '#')[OFFSET(1)] AS participation_status_descriptor,
                JSON_VALUE(statuses, '$.designatedBy') AS designated_by,
                PARSE_DATE('%Y-%m-%d', JSON_VALUE(statuses, "$.statusBeginDate")) AS status_begin_date,
                PARSE_DATE('%Y-%m-%d', JSON_VALUE(statuses, "$.statusEndDate")) AS status_end_date
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.programParticipationStatuses")) statuses 
        ) AS program_participation_statuses,
    FROM {{ source('staging', 'base_edfi_student_special_education_program_associations') }}
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
