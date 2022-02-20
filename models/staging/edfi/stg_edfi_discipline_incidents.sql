
WITH parsed_data AS (
    SELECT
        JSON_VALUE(data, '$.extractedTimestamp') AS extracted_timestamp,
        JSON_VALUE(data, '$.id') AS id,
        CAST(JSON_VALUE(data, '$.schoolYear') AS int64) school_year,
        JSON_VALUE(data, '$.incidentIdentifier') AS incident_identifier,
        JSON_VALUE(data, '$.caseNumber') AS case_number,
        CAST(JSON_VALUE(data, '$.incidentCost') AS float64) AS incident_cost,
        PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, '$.incidentDate')) AS incident_date,
        JSON_VALUE(data, '$.incidentDescription') AS incident_description,
        SPLIT(JSON_VALUE(data, '$.incidentLocationDescriptor'), '#')[OFFSET(1)] AS incident_location_descriptor,
        JSON_VALUE(data, '$.incidentTime') AS incident_time,
        CAST(JSON_VALUE(data, '$.reportedToLawEnforcement') AS BOOL) AS reported_to_law_enforcement,
        SPLIT(JSON_VALUE(data, '$.reporterDescriptionDescriptor'), '#')[OFFSET(1)] AS reporter_description_descriptor,
        JSON_VALUE(data, '$.reporterName') AS reporter_name,
        STRUCT(
            JSON_VALUE(data, '$.schoolReference.schoolId') AS school_id
        ) AS school_reference,
        STRUCT(
            JSON_VALUE(data, '$.staffReference.staffUniqueId') AS staff_unique_id
        ) AS staff_reference,
        ARRAY(
            SELECT AS STRUCT 
                SPLIT(JSON_VALUE(behaviors, '$.behaviorDescriptor'), '#')[OFFSET(1)] AS behavior_descriptor,
                JSON_VALUE(behaviors, "$.behaviorDetailedDescription") AS behavior_detailed_description
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.behaviors")) behaviors 
        ) AS behaviors,
        ARRAY(
            SELECT AS STRUCT 
                SPLIT(JSON_VALUE(external_participants, '$.disciplineIncidentParticipationCodeDescriptor'), '#')[OFFSET(1)] AS discipline_incident_participation_code_descriptor,
                JSON_VALUE(external_participants, "$.firstName") AS first_name,
                JSON_VALUE(external_participants, "$.lastSurname") AS last_surname
            FROM UNNEST(JSON_QUERY_ARRAY(data, "$.externalParticipants")) external_participants 
        ) AS external_participants,
    FROM {{ source('staging', 'base_edfi_discipline_incidents') }}

),

ranked AS (

    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY
                school_year,
                school_reference.school_id,
                incident_identifier
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
