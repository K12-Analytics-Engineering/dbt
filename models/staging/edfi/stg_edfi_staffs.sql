
WITH parsed_data AS (

    SELECT
        JSON_VALUE(data, '$.extractedTimestamp') AS extracted_timestamp,
        JSON_VALUE(data, '$.id') AS id,
        CAST(JSON_VALUE(data, '$.schoolYear') AS int64) school_year,
        JSON_VALUE(data, '$.staffUniqueId') AS staff_unique_id,
        JSON_VALUE(data, '$.lastSurname') AS last_surname,
        JSON_VALUE(data, '$.middleName') AS middle_name,
        JSON_VALUE(data, '$.firstName') AS first_name,
        STRUCT(
            JSON_VALUE(data, '$.personReference.personId') AS person_id,
            SPLIT(JSON_VALUE(data, "$.personReference.sourceSystemDescriptor"), '#')[OFFSET(1)] AS source_system_descriptor
        ) AS person_reference,
        JSON_VALUE(data, '$.generationCodeSuffix') AS generation_code_suffix,
        JSON_VALUE(data, '$.loginId') AS login_id,
        PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, '$.birthDate')) AS birth_date,
        SPLIT(JSON_VALUE(data, "$.citizenshipStatusDescriptor"), '#')[OFFSET(1)] AS citizenship_status_descriptor,
        SPLIT(JSON_VALUE(data, "$.highestCompletedLevelOfEducationDescriptor"), '#')[OFFSET(1)] AS highest_completed_level_of_education_descriptor,
        CAST(JSON_VALUE(data, "$.highlyQualifiedTeacher") AS BOOL) AS highly_qualified_teacher,
        CAST(JSON_VALUE(data, "$.hispanicLatinoEthnicity") AS BOOL) AS hispanic_latino_ethnicity,
        ARRAY(
            SELECT AS STRUCT 
                SPLIT(JSON_VALUE(electronic_mails, '$.electronicMailTypeDescriptor'), '#')[OFFSET(1)] AS electronic_mail_type_descriptor,
                JSON_VALUE(electronic_mails, "$.electronicMailAddress") AS electronic_mail_address,
                CAST(JSON_VALUE(electronic_mails, "$.doNotPublishIndicator") AS BOOL) AS do_not_publish_indicator
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.electronicMails")) electronic_mails 
        ) AS electronic_mails,
        ARRAY(
            SELECT AS STRUCT 
                SPLIT(JSON_VALUE(codes, '$.staffIdentificationSystemDescriptor'), '#')[OFFSET(1)] AS staff_identification_system_descriptor,
                JSON_VALUE(codes, "$.assigningOrganizationIdentificationCode") AS assigning_organization_identification_code,
                JSON_VALUE(codes, "$.identificationCode") AS identification_code
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.identificationCodes")) codes 
        ) AS identification_codes,
        ARRAY(
            SELECT AS STRUCT 
                SPLIT(JSON_VALUE(descriptors, '$.ancestryEthnicOriginDescriptor'), '#')[OFFSET(1)] AS ancestry_ethnic_origin_descriptor,
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.ancestry_ethnic_origins")) descriptors 
        ) AS ancestry_ethnic_origins,
        ARRAY(
            SELECT AS STRUCT 
                SPLIT(JSON_VALUE(races, '$.raceDescriptor'), '#')[OFFSET(1)] AS race_descriptor,
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.races")) races 
        ) AS races
    FROM {{ source('staging', 'base_edfi_staffs') }}

),

ranked AS (

    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY
                school_year,
                staff_unique_id
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
