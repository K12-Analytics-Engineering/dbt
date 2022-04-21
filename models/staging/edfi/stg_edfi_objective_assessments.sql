
 {{ retrieve_edfi_records_from_data_lake('base_edfi_objective_assessments') }}

select
    date_extracted                          as date_extracted,
    school_year                             as school_year,
    id                                      as id,
    json_value(data, '$.identificationCode') as identification_code,
    split(json_value(data, "$.academicSubjectDescriptor"), '#')[OFFSET(1)] as academic_subject_descriptor,
    json_value(data, '$.description') as description,
    cast(json_value(data, '$.maxRawScore') as float64) as max_raw_score,
    cast(json_value(data, '$.percentOfAssessment') as float64) as percent_of_assessment,
    json_value(data, '$.nomenclature') as nomenclature,
    struct(
        json_value(data, '$.assessmentReference.assessmentIdentifier') as assessment_identifier,
        json_value(data, '$.assessmentReference.namespace') as namespace
    ) as assessment_reference,
    struct(
        json_value(data, '$.parentObjectiveAssessmentReference.assessmentIdentifier') as assessment_identifier,
        json_value(data, '$.parentObjectiveAssessmentReference.identificationCode') as identification_code,
        json_value(data, '$.parentObjectiveAssessmentReference.namespace') as namespace
    ) as parent_objective_assessment_reference,
    array(
        select as struct 
            json_value(assessment_items, '$.assessmentItemReference.assessmentIdentifier') as assessment_identifier,
            json_value(assessment_items, '$.assessmentItemReference.identificationCode') as identification_code,
            json_value(assessment_items, '$.assessmentItemReference.namespace') as namespace
        from unnest(json_query_array(data, "$.assessmentItems")) assessment_items 
    ) as assessment_items,
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
    array(
        select as struct 
            split(json_value(performance_levels, "$.assessmentReportingMethodDescriptor"), '#')[OFFSET(1)] as assessment_reporting_method_descriptor,
            split(json_value(performance_levels, "$.performanceLevelDescriptor"), '#')[OFFSET(1)] as performance_level_descriptor,
            split(json_value(performance_levels, "$.resultDatatypeTypeDescriptor"), '#')[OFFSET(1)] as result_datatype_type_descriptor,
            json_value(performance_levels, "$.maximumScore") as maximum_score,
            json_value(performance_levels, "$.minimumScore") as minimum_score
        from unnest(json_query_array(data, "$.performanceLevels")) performance_levels 
    ) as performance_levels,
    array(
        select as struct 
            split(json_value(scores, "$.assessmentReportingMethodDescriptor"), '#')[OFFSET(1)] as assessment_reporting_method_descriptor,
            split(json_value(scores, "$.resultDatatypeTypeDescriptor"), '#')[OFFSET(1)] as result_datatype_type_descriptor,
            json_value(scores, "$.maximumScore") as maximum_score,
            json_value(scores, "$.minimumScore") as minimum_score
        from unnest(json_query_array(data, "$.scores")) scores 
    ) as scores
from records

{{ remove_edfi_deletes_and_duplicates() }}
