
{{ retrieve_edfi_records_from_data_lake('base_edfi_student_program_associations') }}

SELECT
    date_extracted                          AS date_extracted,
    school_year                             AS school_year,
    id                                      AS id,
    PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, "$.beginDate")) AS begin_date,
    PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, "$.endDate")) AS end_date,
    STRUCT(
        JSON_VALUE(data, '$.educationOrganizationReference.educationOrganizationId') AS education_organization_id
    ) AS education_organization_reference,
    SPLIT(JSON_VALUE(data, "$.reasonExitedDescriptor"), '#')[OFFSET(1)] AS reason_exited_descriptor,
    CAST(JSON_VALUE(data, '$.servedOutsideOfRegularSession') AS BOOL) served_outside_of_regular_session,
    STRUCT(
        JSON_VALUE(data, '$.programReference.educationOrganizationId') AS education_organization_id,
        JSON_VALUE(data, '$.programReference.programName') AS program_name,
        SPLIT(JSON_VALUE(data, "$.programReference.programTypeDescriptor"), '#')[OFFSET(1)] AS program_type_descriptor
    ) AS program_reference,
    STRUCT(
        JSON_VALUE(data, '$.studentReference.studentUniqueId') AS student_unique_id
    ) AS student_reference,
    STRUCT(
        SPLIT(JSON_VALUE(data, "$.participationStatus.participationStatusDescriptor"), '#')[OFFSET(1)] AS participation_status_descriptor,
        JSON_VALUE(data, '$.participationStatus.designatedBy') AS designated_by,
        PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, "$.participationStatus.statusBeginDate")) AS status_begin_date,
        PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, "$.participationStatus.statusEndDate")) AS status_end_date
    ) AS participation_status,
    ARRAY(
        SELECT AS STRUCT 
            SPLIT(JSON_VALUE(statuses, "$.participationStatusDescriptor"), '#')[OFFSET(1)] AS participation_status_descriptor,
            JSON_VALUE(statuses, '$.designatedBy') AS designated_by,
            PARSE_DATE('%Y-%m-%d', JSON_VALUE(statuses, "$.statusBeginDate")) AS status_begin_date,
            PARSE_DATE('%Y-%m-%d', JSON_VALUE(statuses, "$.statusEndDate")) AS status_end_date
        FROM UNNEST(JSON_QUERY_ARRAY(data, "$.programParticipationStatuses")) statuses 
    ) AS program_participation_statuses,
    ARRAY(
        SELECT AS STRUCT 
            SPLIT(JSON_VALUE(services, "$.serviceDescriptor"), '#')[OFFSET(1)] AS service_descriptor,
            CAST(JSON_VALUE(services, '$.primaryIndicator') AS BOOL) primary_indicator,
            PARSE_DATE('%Y-%m-%d', JSON_VALUE(services, "$.statusBeginDate")) AS status_begin_date,
            PARSE_DATE('%Y-%m-%d', JSON_VALUE(services, "$.statusEndDate")) AS status_end_date
        FROM UNNEST(JSON_QUERY_ARRAY(data, "$.services")) services 
    ) AS services
FROM records

{{ remove_edfi_deletes_and_duplicates() }}
