
{{ retrieve_edfi_records_from_data_lake('base_edfi_student_special_education_program_associations') }}

select
    date_extracted                          as date_extracted,
    school_year                             as school_year,
    id                                      as id,
    parse_date('%Y-%m-%d', json_value(data, "$.beginDate")) as begin_date,
    parse_date('%Y-%m-%d', json_value(data, "$.endDate")) as end_date,
    parse_date('%Y-%m-%d', json_value(data, "$.iepBeginDate")) as iep_begin_date,
    parse_date('%Y-%m-%d', json_value(data, "$.iepEndDate")) as iep_end_date,
    parse_date('%Y-%m-%d', json_value(data, "$.iepReviewDate")) as iep_review_date,
    parse_date('%Y-%m-%d', json_value(data, "$.lastEvaluationDate")) as last_evaluation_date,
    cast(json_value(data, '$.ideaEligibility') as BOOL) idea_eligibility,
    cast(json_value(data, '$.medicallyFragile') as BOOL) medically_fragile,
    cast(json_value(data, '$.multiplyDisabled') as BOOL) multiply_disabled,
    cast(json_value(data, '$.servedOutsideOfRegularSession') as BOOL) served_outside_of_regular_session,
    split(json_value(data, "$.reasonExitedDescriptor"), '#')[OFFSET(1)] as reason_exited_descriptor,
    split(json_value(data, "$.specialEducationSettingDescriptor"), '#')[OFFSET(1)] as special_education_setting_descriptor,
    cast(json_value(data, '$.schoolHoursPerWeek') as float64) as school_hours_per_week,
    cast(json_value(data, '$.specialEducationHoursPerWeek') as float64) as special_education_hours_per_week,
    struct(
        json_value(data, '$.educationOrganizationReference.educationOrganizationId') as education_organization_id
    ) as education_organization_reference,
    struct(
        json_value(data, '$.programReference.educationOrganizationId') as education_organization_id,
        json_value(data, '$.programReference.programName') as program_name,
        split(json_value(data, "$.programReference.programTypeDescriptor"), '#')[OFFSET(1)] as program_type_descriptor
    ) as program_reference,
    struct(
        json_value(data, '$.studentReference.studentUniqueId') as student_unique_id
    ) as student_reference,
    array(
        select as struct 
            split(json_value(disabilities, "$.disabilityDescriptor"), '#')[OFFSET(1)] as disability_descriptor,
            split(json_value(disabilities, "$.disabilityDeterminationSourceTypeDescriptor"), '#')[OFFSET(1)] as disability_determination_source_type_descriptor,
            json_value(disabilities, '$.disabilityDiagnosis') as disability_diagnosis,
            cast(json_value(disabilities, '$.orderOfDisability') as int64) as order_of_disability
            -- designations array
        from unnest(json_query_array(data, "$.disabilities")) disabilities 
    ) as disabilities,
    struct(
        split(json_value(data, "$.participationStatusDescriptor"), '#')[OFFSET(1)] as participation_status_descriptor,
        json_value(data, '$.designatedBy') as designated_by,
        parse_date('%Y-%m-%d', json_value(data, "$.statusBeginDate")) as status_begin_date,
        parse_date('%Y-%m-%d', json_value(data, "$.statusEndDate")) as status_end_date
    ) as participation_status,
    array(
        select as struct 
            split(json_value(statuses, "$.participationStatusDescriptor"), '#')[OFFSET(1)] as participation_status_descriptor,
            json_value(statuses, '$.designatedBy') as designated_by,
            parse_date('%Y-%m-%d', json_value(statuses, "$.statusBeginDate")) as status_begin_date,
            parse_date('%Y-%m-%d', json_value(statuses, "$.statusEndDate")) as status_end_date
        from unnest(json_query_array(data, "$.programParticipationStatuses")) statuses 
    ) as program_participation_statuses,
from records

{{ remove_edfi_deletes_and_duplicates() }}
