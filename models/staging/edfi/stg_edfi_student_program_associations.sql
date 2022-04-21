
{{ retrieve_edfi_records_from_data_lake('base_edfi_student_program_associations') }}

select
    date_extracted                          as date_extracted,
    school_year                             as school_year,
    id                                      as id,
    parse_date('%Y-%m-%d', json_value(data, "$.beginDate")) as begin_date,
    parse_date('%Y-%m-%d', json_value(data, "$.endDate")) as end_date,
    struct(
        json_value(data, '$.educationOrganizationReference.educationOrganizationId') as education_organization_id
    ) as education_organization_reference,
    split(json_value(data, "$.reasonExitedDescriptor"), '#')[OFFSET(1)] as reason_exited_descriptor,
    cast(json_value(data, '$.servedOutsideOfRegularSession') as BOOL) served_outside_of_regular_session,
    struct(
        json_value(data, '$.programReference.educationOrganizationId') as education_organization_id,
        json_value(data, '$.programReference.programName') as program_name,
        split(json_value(data, "$.programReference.programTypeDescriptor"), '#')[OFFSET(1)] as program_type_descriptor
    ) as program_reference,
    struct(
        json_value(data, '$.studentReference.studentUniqueId') as student_unique_id
    ) as student_reference,
    struct(
        split(json_value(data, "$.participationStatus.participationStatusDescriptor"), '#')[OFFSET(1)] as participation_status_descriptor,
        json_value(data, '$.participationStatus.designatedBy') as designated_by,
        parse_date('%Y-%m-%d', json_value(data, "$.participationStatus.statusBeginDate")) as status_begin_date,
        parse_date('%Y-%m-%d', json_value(data, "$.participationStatus.statusEndDate")) as status_end_date
    ) as participation_status,
    array(
        select as struct 
            split(json_value(statuses, "$.participationStatusDescriptor"), '#')[OFFSET(1)] as participation_status_descriptor,
            json_value(statuses, '$.designatedBy') as designated_by,
            parse_date('%Y-%m-%d', json_value(statuses, "$.statusBeginDate")) as status_begin_date,
            parse_date('%Y-%m-%d', json_value(statuses, "$.statusEndDate")) as status_end_date
        from unnest(json_query_array(data, "$.programParticipationStatuses")) statuses 
    ) as program_participation_statuses,
    array(
        select as struct 
            split(json_value(services, "$.serviceDescriptor"), '#')[OFFSET(1)] as service_descriptor,
            cast(json_value(services, '$.primaryIndicator') as BOOL) primary_indicator,
            parse_date('%Y-%m-%d', json_value(services, "$.statusBeginDate")) as status_begin_date,
            parse_date('%Y-%m-%d', json_value(services, "$.statusEndDate")) as status_end_date
        from unnest(json_query_array(data, "$.services")) services 
    ) as services
from records

{{ remove_edfi_deletes_and_duplicates() }}
