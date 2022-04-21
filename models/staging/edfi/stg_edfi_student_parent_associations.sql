
{{ retrieve_edfi_records_from_data_lake('base_edfi_student_parent_associations') }}

select
    date_extracted                          as date_extracted,
    school_year                             as school_year,
    id                                      as id,
    struct(
        json_value(data, '$.parentReference.parentUniqueId') as parent_unique_id
    ) as parent_reference,
    struct(
        json_value(data, '$.studentReference.studentUniqueId') as student_unique_id
    ) as student_reference,
    cast(json_value(data, '$.contactPriority') as int64) as contact_priority,
    json_value(data, '$.contactRestrictions') as contact_restrictions,
    cast(json_value(data, '$.emergencyContactStatus') as BOOL) as emergency_contact_status,
    cast(json_value(data, '$.legalGuardian') as BOOL) as legal_guardian,
    cast(json_value(data, '$.livesWith') as BOOL) as lives_with,
    cast(json_value(data, '$.primaryContactStatus') as BOOL) as primary_contact_status,
    split(json_value(data, '$.relationDescriptor'), '#')[OFFSET(1)] as relation_descriptor
from records

{{ remove_edfi_deletes_and_duplicates() }}
