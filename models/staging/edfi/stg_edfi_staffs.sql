
{{ retrieve_edfi_records_from_data_lake('base_edfi_staffs') }}

select
    date_extracted                          as date_extracted,
    school_year                             as school_year,
    id                                      as id,
    json_value(data, '$.staffUniqueId') as staff_unique_id,
    json_value(data, '$.lastSurname') as last_surname,
    json_value(data, '$.middleName') as middle_name,
    json_value(data, '$.firstName') as first_name,
    struct(
        json_value(data, '$.personReference.personId') as person_id,
        split(json_value(data, "$.personReference.sourceSystemDescriptor"), '#')[OFFSET(1)] as source_system_descriptor
    ) as person_reference,
    json_value(data, '$.generationCodeSuffix') as generation_code_suffix,
    json_value(data, '$.loginId') as login_id,
    parse_date('%Y-%m-%d', json_value(data, '$.birthDate')) as birth_date,
    split(json_value(data, "$.citizenshipStatusDescriptor"), '#')[OFFSET(1)] as citizenship_status_descriptor,
    split(json_value(data, "$.highestCompletedLevelOfEducationDescriptor"), '#')[OFFSET(1)] as highest_completed_level_of_education_descriptor,
    cast(json_value(data, "$.highlyQualifiedTeacher") as BOOL) as highly_qualified_teacher,
    cast(json_value(data, "$.hispanicLatinoEthnicity") as BOOL) as hispanic_latino_ethnicity,
    array(
        select as struct 
            split(json_value(electronic_mails, '$.electronicMailTypeDescriptor'), '#')[OFFSET(1)] as electronic_mail_type_descriptor,
            json_value(electronic_mails, "$.electronicMailAddress") as electronic_mail_address,
            cast(json_value(electronic_mails, "$.doNotPublishIndicator") as BOOL) as do_not_publish_indicator
        from unnest(json_query_array(data, "$.electronicMails")) electronic_mails 
    ) as electronic_mails,
    array(
        select as struct 
            split(json_value(codes, '$.staffIdentificationSystemDescriptor'), '#')[OFFSET(1)] as staff_identification_system_descriptor,
            json_value(codes, "$.assigningOrganizationIdentificationCode") as assigning_organization_identification_code,
            json_value(codes, "$.identificationCode") as identification_code
        from unnest(json_query_array(data, "$.identificationCodes")) codes 
    ) as identification_codes,
    array(
        select as struct 
            split(json_value(descriptors, '$.ancestryEthnicOriginDescriptor'), '#')[OFFSET(1)] as ancestry_ethnic_origin_descriptor,
        from unnest(json_query_array(data, "$.ancestry_ethnic_origins")) descriptors 
    ) as ancestry_ethnic_origins,
    array(
        select as struct 
            split(json_value(races, '$.raceDescriptor'), '#')[OFFSET(1)] as race_descriptor,
        from unnest(json_query_array(data, "$.races")) races 
    ) as races
from records

{{ remove_edfi_deletes_and_duplicates() }}
