
{{ retrieve_edfi_records_from_data_lake('base_edfi_grading_periods') }}

select
    date_extracted                          as date_extracted,
    school_year                             as school_year,
    id                                      as id,
    split(json_value(data, "$.gradingPeriodDescriptor"), '#')[OFFSET(1)] as grading_period_descriptor,
    cast(json_value(data, "$.periodSequence") as int64) as period_sequence,
    struct(
        json_value(data, '$.schoolReference.schoolId') as school_id
    ) as school_reference,
    struct(
        cast(json_value(data, '$.schoolYearTypeReference.schoolYear') as int64) as school_year
    ) as school_year_type_reference,
    parse_date('%Y-%m-%d', json_value(data, "$.beginDate")) as begin_date,
    parse_date('%Y-%m-%d', json_value(data, "$.endDate")) as end_date,
    cast(json_value(data, "$.totalInstructionalDays") as int64) as total_instructional_days
from records

{{ remove_edfi_deletes_and_duplicates() }}
