
 {{ retrieve_edfi_records_from_data_lake('base_edfi_parents') }}

select
    date_extracted                          as date_extracted,
    school_year                             as school_year,
    id                                      as id,
    json_value(data, '$.parentUniqueId') as parent_unique_id,
    json_value(data, '$.lastSurname') as last_surname,
    json_value(data, '$.middleName') as middle_name,
    json_value(data, '$.firstName') as first_name,
    struct(
        json_value(data, '$.personReference.personId') as person_id,
        split(json_value(data, "$.personReference.sourceSystemDescriptor"), '#')[OFFSET(1)] as source_system_descriptor
    ) as person_reference,
    json_value(data, '$.generationCodeSuffix') as generation_code_suffix,
    array(
        select as struct 
            split(json_value(electronic_mails, '$.electronicMailTypeDescriptor'), '#')[OFFSET(1)] as electronic_mail_type_descriptor,
            json_value(electronic_mails, "$.electronicMailAddress") as electronic_mail_address,
            cast(json_value(electronic_mails, "$.doNotPublishIndicator") as BOOL) as do_not_publish_indicator
        from unnest(json_query_array(data, "$.electronicMails")) electronic_mails 
    ) as electronic_mails,
    array(
        select as struct 
            split(json_value(addresses, '$.addressTypeDescriptor'), '#')[OFFSET(1)] as address_type_descriptor,
            split(json_value(addresses, '$.stateAbbreviationDescriptor'), '#')[OFFSET(1)] as state_abbreviation_descriptor,
            json_value(addresses, "$.city") as city,
            json_value(addresses, "$.postalCode") as postal_code,
            json_value(addresses, "$.streetNumberName") as street_number_name,
            split(json_value(addresses, '$.localeDescriptor'), '#')[OFFSET(1)] as locale_descriptor,
            json_value(addresses, "$.apartmentRoomSuiteNumber") as apartment_room_suite_number,
            json_value(addresses, "$.buildingSiteNumber") as building_site_number,
            json_value(addresses, "$.congressionalDistrict") as congressional_district,
            json_value(addresses, "$.countyFIPSCode") as county_fips_code,
            cast(json_value(addresses, "$.doNotPublishIndicator") as BOOL) as do_not_publish_indicator,
            json_value(addresses, "$.latitude") as latitude,
            json_value(addresses, "$.longitude") as longitude,
            json_value(addresses, "$.nameOfCounty") as name_of_county,
        from unnest(json_query_array(data, "$.addresses")) addresses 
    ) as addresses,
    array(
        select as struct 
            split(json_value(telephones, '$.telephoneNumberTypeDescriptor'), '#')[OFFSET(1)] as telephone_number_type_descriptor,
            json_value(telephones, "$.telephoneNumber") as telephone_number,
            cast(json_value(telephones, "$.doNotPublishIndicator") as BOOL) as do_not_publish_indicator,
            cast(json_value(telephones, "$.orderOfPriority") as int64) as order_of_priority,
            cast(json_value(telephones, "$.textMessageCapabilityIndicator") as BOOL) as text_message_capability_indicator
        from unnest(json_query_array(data, "$.telephones")) telephones 
    ) as telephones,
from records

{{ remove_edfi_deletes_and_duplicates() }}
