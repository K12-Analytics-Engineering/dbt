
{{ retrieve_edfi_records_from_data_lake('base_edfi_student_school_associations') }}

select
    date_extracted                          as date_extracted,
    school_year                             as school_year,
    json_value(data, '$.id') as id,
    struct(
        json_value(data, '$.schoolReference.schoolId') as school_id
    ) as school_reference,
    struct(
        json_value(data, '$.studentReference.studentUniqueId') as student_unique_id
    ) as student_reference,
    struct(
        cast(json_value(data, '$.schoolYearTypeReference.schoolYear') as int64) as school_year
    ) as school_year_type_reference,
    split(json_value(data, '$.entryTypeDescriptor'), '#')[OFFSET(1)] as entry_type_descriptor,
    split(json_value(data, '$.entryGradeLevelDescriptor'), '#')[OFFSET(1)] as entry_grade_level_descriptor,
    parse_date('%Y-%m-%d', json_value(data, '$.entryDate')) as entry_date,
    parse_date('%Y-%m-%d', json_value(data, '$.exitWithdrawDate')) as exit_withdraw_date,
    split(json_value(data, '$.exitWithdrawTypeDescriptor'), '#')[OFFSET(1)] as exit_withdraw_type_descriptor,
    cast(json_value(data, '$.fullTimeEquivalency') as int64) as full_time_equivalency,
    cast(json_value(data, '$.primarySchool') as BOOL) as primary_school,
    cast(json_value(data, '$.repeatGradeIndicator') as BOOL) as repeat_grade_indicator,
    cast(json_value(data, '$.schoolChoiceTransfer') as BOOL) as school_choice_transfer,
    cast(json_value(data, '$.termCompletionIndicator') as BOOL) as term_completion_indicator
from records

{{ remove_edfi_deletes_and_duplicates() }}
