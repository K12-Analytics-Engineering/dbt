
WITH parsed_data AS (

    SELECT
        JSON_VALUE(data, '$.extractedTimestamp') AS extracted_timestamp,
        JSON_VALUE(data, '$.id') AS id,
        CAST(JSON_VALUE(data, '$.schoolYear') AS int64) school_year,
        STRUCT(
            JSON_VALUE(data, '$.disciplineIncidentReference.incidentIdentifier') AS incident_identifier,
            JSON_VALUE(data, '$.disciplineIncidentReference.schoolId') AS school_id
        ) AS discipline_incident_reference,
        SPLIT(JSON_VALUE(data, '$.studentParticipationCodeDescriptor'), '#')[OFFSET(1)] AS student_participation_code_descriptor,
        STRUCT(
            JSON_VALUE(data, '$.studentReference.studentUniqueId') AS student_unique_id
        ) AS student_reference,
        ARRAY(
            SELECT AS STRUCT 
                SPLIT(JSON_VALUE(behaviors, '$.behaviorDescriptor'), '#')[OFFSET(1)] AS behavior_descriptor,
                JSON_VALUE(behaviors, "$.behaviorDetailedDescription") AS behavior_detailed_description
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.behaviors")) behaviors 
        ) AS behaviors
    FROM {{ source('staging', 'base_edfi_student_discipline_incident_associations') }}

),

ranked AS (

    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY
                school_year,
                discipline_incident_reference.school_id,
                discipline_incident_reference.incident_identifier,
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
