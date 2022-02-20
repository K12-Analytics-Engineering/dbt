
WITH parsed_data AS (

    SELECT
        JSON_VALUE(data, '$.extractedTimestamp') AS extracted_timestamp,
        JSON_VALUE(data, '$.id') AS id,
        CAST(JSON_VALUE(data, '$.schoolYear') AS int64) school_year,
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

),

ranked AS (

    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY
                school_year,
                parent_reference.parent_unique_id,
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
