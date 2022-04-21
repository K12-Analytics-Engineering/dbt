
{{ retrieve_edfi_records_from_data_lake('base_edfi_staff_education_organization_assignment_associations') }}

select
    date_extracted                          as date_extracted,
    school_year                             as school_year,
    id                                      as id,
    struct(
        json_value(data, '$.staffReference.staffUniqueId') as staff_unique_id
    ) as staff_reference,
    split(json_value(data, "$.staffClassificationDescriptor"), '#')[OFFSET(1)] as staff_classification_descriptor,
    struct(
        json_value(data, '$.educationOrganizationReference.educationOrganizationId') as education_organization_id
    ) as education_organization_reference,
    parse_date('%Y-%m-%d', json_value(data, '$.beginDate')) as begin_date,
    parse_date('%Y-%m-%d', json_value(data, '$.endDate')) as end_date
from records

{{ remove_edfi_deletes_and_duplicates() }}
