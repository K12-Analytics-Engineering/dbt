
{{ retrieve_edfi_records_from_data_lake('base_edfi_student_discipline_incident_associations') }}

select
    date_extracted                          as date_extracted,
    school_year                             as school_year,
    id                                      as id,
    struct(
        json_value(data, '$.disciplineIncidentReference.incidentIdentifier') as incident_identifier,
        json_value(data, '$.disciplineIncidentReference.schoolId') as school_id
    ) as discipline_incident_reference,
    split(json_value(data, '$.studentParticipationCodeDescriptor'), '#')[OFFSET(1)] as student_participation_code_descriptor,
    struct(
        json_value(data, '$.studentReference.studentUniqueId') as student_unique_id
    ) as student_reference,
    array(
        select as struct 
            split(json_value(behaviors, '$.behaviorDescriptor'), '#')[OFFSET(1)] as behavior_descriptor,
            json_value(behaviors, "$.behaviorDetailedDescription") as behavior_detailed_description
        from unnest(json_query_array(data, "$.behaviors")) behaviors 
    ) as behaviors
from records

{{ remove_edfi_deletes_and_duplicates() }}
