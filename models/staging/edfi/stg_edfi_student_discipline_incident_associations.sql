
{{ retrieve_edfi_records_from_data_lake('base_edfi_student_discipline_incident_associations') }}

SELECT
    date_extracted                          AS date_extracted,
    school_year                             AS school_year,
    id                                      AS id,
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
FROM records

{{ remove_edfi_deletes_and_duplicates() }}
