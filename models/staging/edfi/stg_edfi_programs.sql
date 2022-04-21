
 {{ retrieve_edfi_records_from_data_lake('base_edfi_programs') }}

select
    date_extracted                          as date_extracted,
    school_year                             as school_year,
    id                                      as id,
    json_value(data, '$.programName') as program_name,
    json_value(data, '$.programId') as program_id,
    split(json_value(data, '$.programTypeDescriptor'), '#')[OFFSET(1)] as program_type_descriptor,
    struct(
        json_value(data, '$.educationOrganizationReference.educationOrganizationId') as education_organization_id
    ) as education_organization_reference,
    array(
        select as struct 
            split(json_value(services, "$.serviceDescriptor"), '#')[OFFSET(1)] as service_descriptor,
        from unnest(json_query_array(data, "$.services")) services 
    ) as services,
    array(
        select as struct 
            split(json_value(sponsors, "$.programSponsorDescriptor"), '#')[OFFSET(1)] as program_sponsor_descriptor,
        from unnest(json_query_array(data, "$.sponsors")) sponsors 
    ) as sponsors,
    array(
        select as struct 
            split(json_value(characteristics, "$.programCharacteristicDescriptor"), '#')[OFFSET(1)] as program_characteristic_descriptor,
        from unnest(json_query_array(data, "$.characteristics")) characteristics 
    ) as characteristics,
    array(
        select as struct
            struct(
                json_value(learning_objectives, '$.learningObjectiveReference.learningObjectiveId') as learning_objective_id,
                json_value(learning_objectives, '$.learningObjectiveReference.namespace') as namespace
            ) as learning_objective_reference
        from unnest(json_query_array(data, "$.learningObjectives")) learning_objectives 
    ) as learning_objectives,
    array(
            select as struct
                struct(
                    json_value(learning_standards, '$.learningStandardReference.learningStandardId') as learning_standard_id
                ) as learning_standard_reference
            from unnest(json_query_array(data, "$.learningStandards")) learning_standards
    ) as learning_standards,
    json_value(data, '$.schoolId') as school_id,
    json_value(data, '$.nameOfInstitution') as name_of_institution,
    split(json_value(data, '$.schoolTypeDescriptor'), '#')[OFFSET(1)] as school_type_descriptor,
from records

{{ remove_edfi_deletes_and_duplicates() }}
