
WITH parsed_data AS (

    SELECT
        JSON_VALUE(data, '$.extractedTimestamp') AS extracted_timestamp,
        JSON_VALUE(data, '$.id') AS id,
        CAST(JSON_VALUE(data, '$.schoolYear') AS int64) school_year,
        STRUCT(
            JSON_VALUE(data, '$.educationOrganizationReference.educationOrganizationId') AS education_organization_id
        ) AS education_organization_reference,
        STRUCT(
            JSON_VALUE(data, '$.studentReference.studentUniqueId') AS student_unique_id
        ) AS student_reference,
        ARRAY(
            SELECT AS STRUCT 
                JSON_VALUE(email, "$.electronicMailAddress") AS address,
                SPLIT(JSON_VALUE(email, "$.electronicMailTypeDescriptor"), '#')[OFFSET(1)] AS type_descriptor,
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.electronicMails")) email 
        ) AS electronic_mail,
        SPLIT(JSON_VALUE(data, '$.limitedEnglishProficiencyDescriptor'), '#')[OFFSET(1)] AS limited_english_proficiency_descriptor,
        CAST(JSON_VALUE(data, '$.hispanicLatinoEthnicity') AS BOOL) AS hispanic_latino_ethnicity,
        SPLIT(JSON_VALUE(data, '$.sexDescriptor'), '#')[OFFSET(1)] AS sex_descriptor,
        ARRAY(
            SELECT AS STRUCT 
                JSON_VALUE(student_indicators, "$.indicatorName") AS name,
                JSON_VALUE(student_indicators, "$.designatedBy") AS designated_by,
                JSON_VALUE(student_indicators, "$.indicator") AS indicator,
                JSON_VALUE(student_indicators, "$.indicatorGroup") AS indicator_group,
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.studentIndicators")) student_indicators 
        ) AS student_indicators,
        ARRAY(
            SELECT AS STRUCT 
                SPLIT(JSON_VALUE(cohort_years, '$.cohortYearTypeDescriptor'), '#')[OFFSET(1)] AS cohort_type_descriptor,
                SPLIT(JSON_VALUE(cohort_years, '$.termDescriptor'), '#')[OFFSET(1)] AS term_descriptor,
                JSON_VALUE(cohort_years, "$.schoolYearTypeReference.schoolYear") AS school_year,
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.cohortYears")) cohort_years 
        ) AS cohort_years,
        ARRAY(
            SELECT AS STRUCT 
                SPLIT(JSON_VALUE(disabilities, '$.disabilityDescriptor'), '#')[OFFSET(1)] AS disability_descriptor,
                SPLIT(JSON_VALUE(disabilities, '$.disabilityDeterminationSourceTypeDescriptor'), '#')[OFFSET(1)] AS disability_determination_source_type_descriptor,
                JSON_VALUE(disabilities, "$.disabilityDiagnosis") AS disability_diagnosis,
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.disabilities")) disabilities 
        ) AS disabilities,
        ARRAY(
            SELECT AS STRUCT 
                SPLIT(JSON_VALUE(languages, '$.languageDescriptor'), '#')[OFFSET(1)] AS language_descriptor,
                ARRAY(
                    SELECT AS STRUCT 
                        SPLIT(JSON_VALUE(uses, '$.languageUseDescriptor'), '#')[OFFSET(1)] AS language_use_descriptor
                    FROM UNNEST(JSON_QUERY_ARRAY(languages, '$.uses')) AS uses
                ) AS uses
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.languages")) languages 
        ) AS languages,
        ARRAY(
            SELECT AS STRUCT 
                SPLIT(JSON_VALUE(races, "$.raceDescriptor"), '#')[OFFSET(1)] AS race_descriptor
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.races")) races 
        ) AS races,
        ARRAY(
            SELECT AS STRUCT
                JSON_VALUE(student_identification_codes, "$.assigningOrganizationIdentificationCode") AS assigning_organization_identification_code,
                JSON_VALUE(student_identification_codes, "$.identificationCode") AS identification_code,
                SPLIT(JSON_VALUE(student_identification_codes, "$.studentIdentificationSystemDescriptor"), '#')[OFFSET(1)] AS student_identification_system_descriptor
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.studentIdentificationCodes")) student_identification_codes 
        ) AS student_identification_codes
    FROM {{ source('staging', 'base_edfi_student_education_organization_associations') }}

),

ranked AS (

    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY
                school_year,
                education_organization_reference.education_organization_id,
                student_reference.student_unique_id
            ORDER BY school_year DESC, extracted_timestamp DESC
        ) AS rank,
        *
    FROM parsed_data

)

SELECT * EXCEPT (extracted_timestamp, rank)
FROM ranked
WHERE
    rank = 1
    AND id NOT IN (
        SELECT id FROM {{ ref('stg_edfi_deletes') }} edfi_deletes
        WHERE ranked.school_year = edfi_deletes.school_year
    )
