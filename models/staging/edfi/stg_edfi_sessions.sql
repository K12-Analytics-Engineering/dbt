
{{ retrieve_edfi_records_from_data_lake('base_edfi_sessions') }}

select
    date_extracted                          as date_extracted,
    school_year                             as school_year,
    id                                      as id,
    json_value(data, '$.sessionName') as session_name,
    struct(
        json_value(data, '$.schoolReference.schoolId') as school_id
    ) as school_reference,
    struct(
        cast(json_value(data, '$.schoolYearTypeReference.schoolYear') as int64) as school_year
    ) as school_year_type_reference,
    split(json_value(data, "$.termDescriptor"), '#')[OFFSET(1)] as term_descriptor,
    cast(json_value(data, "$.totalInstructionalDays") as int64) as total_instructional_days,
    parse_date('%Y-%m-%d', json_value(data, "$.beginDate")) as begin_date,
    parse_date('%Y-%m-%d', json_value(data, "$.endDate")) as end_date,
    array(
        select as struct
            struct(
                split(json_value(grading_periods, '$.gradingPeriodReference.gradingPeriodDescriptor'), '#')[OFFSET(1)] as grading_period_descriptor,
                cast(json_value(grading_periods, "$.gradingPeriodReference.periodSequence") as int64) as period_sequence,
                json_value(grading_periods, "$.gradingPeriodReference.schoolId") as school_id,
                json_value(grading_periods, "$.gradingPeriodReference.schoolYear") as school_year
            ) as grading_period_reference
        from unnest(json_query_array(data, "$.gradingPeriods")) grading_periods 
    ) as grading_periods
from records

{{ remove_edfi_deletes_and_duplicates() }}
