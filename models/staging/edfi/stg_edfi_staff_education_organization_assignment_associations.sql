
{{ retrieve_edfi_records_from_data_lake('base_edfi_staff_education_organization_assignment_associations') }}

SELECT
    date_extracted                          AS date_extracted,
    school_year                             AS school_year,
    id                                      AS id,
    STRUCT(
        JSON_VALUE(data, '$.staffReference.staffUniqueId') AS staff_unique_id
    ) AS staff_reference,
    SPLIT(JSON_VALUE(data, "$.staffClassificationDescriptor"), '#')[OFFSET(1)] AS staff_classification_descriptor,
    STRUCT(
        JSON_VALUE(data, '$.educationOrganizationReference.educationOrganizationId') AS education_organization_id
    ) AS education_organization_reference,
    PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, '$.beginDate')) AS begin_date,
    PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, '$.endDate')) AS end_date
FROM records

{{ remove_edfi_deletes_and_duplicates() }}
