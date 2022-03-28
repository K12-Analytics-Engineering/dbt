
 {{ retrieve_edfi_records_from_data_lake('base_edfi_parents') }}

SELECT
    date_extracted                          AS date_extracted,
    school_year                             AS school_year,
    id                                      AS id,
    JSON_VALUE(data, '$.parentUniqueId') AS parent_unique_id,
    JSON_VALUE(data, '$.lastSurname') AS last_surname,
    JSON_VALUE(data, '$.middleName') AS middle_name,
    JSON_VALUE(data, '$.firstName') AS first_name,
    STRUCT(
        JSON_VALUE(data, '$.personReference.personId') AS person_id,
        SPLIT(JSON_VALUE(data, "$.personReference.sourceSystemDescriptor"), '#')[OFFSET(1)] AS source_system_descriptor
    ) AS person_reference,
    JSON_VALUE(data, '$.generationCodeSuffix') AS generation_code_suffix,
    ARRAY(
        SELECT AS STRUCT 
            SPLIT(JSON_VALUE(electronic_mails, '$.electronicMailTypeDescriptor'), '#')[OFFSET(1)] AS electronic_mail_type_descriptor,
            JSON_VALUE(electronic_mails, "$.electronicMailAddress") AS electronic_mail_address,
            CAST(JSON_VALUE(electronic_mails, "$.doNotPublishIndicator") AS BOOL) AS do_not_publish_indicator
        FROM UNNEST(JSON_QUERY_ARRAY(data, "$.electronicMails")) electronic_mails 
    ) AS electronic_mails,
    ARRAY(
        SELECT AS STRUCT 
            SPLIT(JSON_VALUE(addresses, '$.addressTypeDescriptor'), '#')[OFFSET(1)] AS address_type_descriptor,
            SPLIT(JSON_VALUE(addresses, '$.stateAbbreviationDescriptor'), '#')[OFFSET(1)] AS state_abbreviation_descriptor,
            JSON_VALUE(addresses, "$.city") AS city,
            JSON_VALUE(addresses, "$.postalCode") AS postal_code,
            JSON_VALUE(addresses, "$.streetNumberName") AS street_number_name,
            SPLIT(JSON_VALUE(addresses, '$.localeDescriptor'), '#')[OFFSET(1)] AS locale_descriptor,
            JSON_VALUE(addresses, "$.apartmentRoomSuiteNumber") AS apartment_room_suite_number,
            JSON_VALUE(addresses, "$.buildingSiteNumber") AS building_site_number,
            JSON_VALUE(addresses, "$.congressionalDistrict") AS congressional_district,
            JSON_VALUE(addresses, "$.countyFIPSCode") AS county_fips_code,
            CAST(JSON_VALUE(addresses, "$.doNotPublishIndicator") AS BOOL) AS do_not_publish_indicator,
            JSON_VALUE(addresses, "$.latitude") AS latitude,
            JSON_VALUE(addresses, "$.longitude") AS longitude,
            JSON_VALUE(addresses, "$.nameOfCounty") AS name_of_county,
        FROM UNNEST(JSON_QUERY_ARRAY(data, "$.addresses")) addresses 
    ) AS addresses,
    ARRAY(
        SELECT AS STRUCT 
            SPLIT(JSON_VALUE(telephones, '$.telephoneNumberTypeDescriptor'), '#')[OFFSET(1)] AS telephone_number_type_descriptor,
            JSON_VALUE(telephones, "$.telephoneNumber") AS telephone_number,
            CAST(JSON_VALUE(telephones, "$.doNotPublishIndicator") AS BOOL) AS do_not_publish_indicator,
            CAST(JSON_VALUE(telephones, "$.orderOfPriority") AS int64) AS order_of_priority,
            CAST(JSON_VALUE(telephones, "$.textMessageCapabilityIndicator") AS BOOL) AS text_message_capability_indicator
        FROM UNNEST(JSON_QUERY_ARRAY(data, "$.telephones")) telephones 
    ) AS telephones,
FROM records

{{ remove_edfi_deletes_and_duplicates() }}
