
WITH parsed_data AS (

    SELECT
        date_extracted                          AS date_extracted,
        school_year                             AS school_year,
        JSON_VALUE(data, '$.id') AS id,
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
    WHERE date_extracted >= (
        SELECT MAX(date_extracted) AS date_extracted
        FROM {{ source('staging', 'base_edfi_student_discipline_incident_associations') }}
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
