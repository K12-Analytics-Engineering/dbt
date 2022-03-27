
WITH parsed_data AS (

    SELECT
        date_extracted                          AS date_extracted,
        school_year                             AS school_year,
        JSON_VALUE(data, '$.id')                AS id,
        STRUCT(
            JSON_VALUE(data, '$.parentReference.parentUniqueId') AS parent_unique_id
        ) AS parent_reference,
        STRUCT(
            JSON_VALUE(data, '$.studentReference.studentUniqueId') AS student_unique_id
        ) AS student_reference,
        CAST(JSON_VALUE(data, '$.contactPriority') AS int64) AS contact_priority,
        JSON_VALUE(data, '$.contactRestrictions') AS contact_restrictions,
        CAST(JSON_VALUE(data, '$.emergencyContactStatus') AS BOOL) AS emergency_contact_status,
        CAST(JSON_VALUE(data, '$.legalGuardian') AS BOOL) AS legal_guardian,
        CAST(JSON_VALUE(data, '$.livesWith') AS BOOL) AS lives_with,
        CAST(JSON_VALUE(data, '$.primaryContactStatus') AS BOOL) AS primary_contact_status,
        SPLIT(JSON_VALUE(data, '$.relationDescriptor'), '#')[OFFSET(1)] AS relation_descriptor
    FROM {{ source('staging', 'base_edfi_student_parent_associations') }}
    WHERE date_extracted >= (
        SELECT MAX(date_extracted) AS date_extracted
        FROM {{ source('staging', 'base_edfi_student_parent_associations') }}
        WHERE is_complete_extract IS TRUE)
    QUALIFY ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY date_extracted DESC) = 1

)


SELECT *
FROM parsed_data
WHERE
    id NOT IN (
        SELECT id FROM {{ ref('stg_edfi_deletes') }} edfi_deletes
        WHERE parsed_data.school_year = edfi_deletes.school_year)
