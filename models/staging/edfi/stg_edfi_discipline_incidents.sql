
{{ retrieve_edfi_records_from_data_lake('base_edfi_discipline_incidents') }}

select
    date_extracted                                                              as date_extracted,
    school_year                                                                 as school_year,
    id                                                                          as id,
    json_value(data, '$.incidentIdentifier')                                    as incident_identifier,
    json_value(data, '$.caseNumber')                                            as case_number,
    cast(json_value(data, '$.incidentCost') as float64)                         as incident_cost,
    parse_date('%Y-%m-%d', json_value(data, '$.incidentDate'))                  as incident_date,
    json_value(data, '$.incidentDescription')                                   as incident_description,
    split(json_value(data, '$.incidentLocationDescriptor'), '#')[OFFSET(1)]     as incident_location_descriptor,
    json_value(data, '$.incidentTime')                                          as incident_time,
    cast(json_value(data, '$.reportedToLawEnforcement') as BOOL)                as reported_to_law_enforcement,
    split(json_value(data, '$.reporterDescriptionDescriptor'), '#')[OFFSET(1)]  as reporter_description_descriptor,
    json_value(data, '$.reporterName')                                          as reporter_name,
    struct(
        json_value(data, '$.schoolReference.schoolId') as school_id
    )                                                                           as school_reference,
    struct(
        json_value(data, '$.staffReference.staffUniqueId') as staff_unique_id
    )                                                                           as staff_reference,
    array(
        select as struct 
            split(json_value(behaviors, '$.behaviorDescriptor'), '#')[OFFSET(1)] as behavior_descriptor,
            json_value(behaviors, "$.behaviorDetailedDescription") as behavior_detailed_description
        from unnest(json_query_array(data, "$.behaviors")) behaviors 
    )                                                                           as behaviors,
    array(
        select as struct 
            split(json_value(external_participants, '$.disciplineIncidentParticipationCodeDescriptor'), '#')[OFFSET(1)] as discipline_incident_participation_code_descriptor,
            json_value(external_participants, "$.firstName") as first_name,
            json_value(external_participants, "$.lastSurname") as last_surname
        from unnest(json_query_array(data, "$.externalParticipants")) external_participants 
    )                                                                           as external_participants,
from records

{{ remove_edfi_deletes_and_duplicates() }}
