
{{ retrieve_edfi_records_from_data_lake('base_edfi_student_assessments') }}

select
    date_extracted                          as date_extracted,
    school_year                             as school_year,
    id                                      as id,
    json_value(data, '$.studentAssessmentIdentifier') as student_assessment_identifier,
    EXTRACT(DATE from PARSE_TIMESTAMP('%Y-%m-%dT%TZ', json_value(data, '$.administrationDate'))) as administration_date,
    -- administrationEndDate
    split(json_value(data, "$.administrationEnvironmentDescriptor"), '#')[OFFSET(1)] as administration_environment_descriptor,
    split(json_value(data, "$.administrationLanguageDescriptor"), '#')[OFFSET(1)] as administration_language_descriptor,
    split(json_value(data, "$.eventCircumstanceDescriptor"), '#')[OFFSET(1)] as event_circumstance_descriptor,
    split(json_value(data, "$.platformTypeDescriptor"), '#')[OFFSET(1)] as platform_type_descriptor,
    split(json_value(data, "$.reasonNotTestedDescriptor"), '#')[OFFSET(1)] as reason_not_tested_descriptor,
    split(json_value(data, "$.retestIndicatorDescriptor"), '#')[OFFSET(1)] as retest_indicator_descriptor,
    split(json_value(data, "$.whenAssessedGradeLevelDescriptor"), '#')[OFFSET(1)] as when_assessed_grade_level_descriptor,
    json_value(data, '$.eventDescription') as event_description,
    json_value(data, '$.serialNumber') as serial_number,
    struct(
        json_value(data, '$.assessmentReference.assessmentIdentifier') as assessment_identifier,
        json_value(data, '$.assessmentReference.namespace') as namespace
    ) as assessment_reference,
    struct(
        cast(json_value(data, '$.schoolYearTypeReference.schoolYear') as int64) as school_year
    ) as school_year_type_reference,
    struct(
        json_value(data, '$.studentReference.studentUniqueId') as student_unique_id
    ) as student_reference,
    array(
        select as struct 
            split(json_value(score_results, "$.assessmentReportingMethodDescriptor"), '#')[OFFSET(1)] as assessment_reporting_method_descriptor,
            split(json_value(score_results, "$.resultDatatypeTypeDescriptor"), '#')[OFFSET(1)] as result_datatype_type_descriptor,
            json_value(score_results, '$.result') as result
        from unnest(json_query_array(data, "$.scoreResults")) score_results 
    ) as score_results,
    array(
        select as struct 
            split(json_value(accommodations, "$.accommodationDescriptor"), '#')[OFFSET(1)] as accommodation_descriptor,
        from unnest(json_query_array(data, "$.accommodations")) accommodations 
    ) as accommodations,
    array(
        select as struct 
            split(json_value(items, "$.assessmentItemResultDescriptor"), '#')[OFFSET(1)] as assessment_item_result_descriptor,
            split(json_value(items, "$.responseIndicatorDescriptor"), '#')[OFFSET(1)] as response_indicator_descriptor,
            json_value(items, '$.assessmentResponse') as assessment_response,
            json_value(items, '$.descriptiveFeedback') as descriptive_feedback,
            cast(json_value(items, '$.rawScoreResult') as float64) as raw_score_result,
            json_value(items, '$.timeAssessed') as time_assessed
        from unnest(json_query_array(data, "$.items")) items 
    ) as items,
    array(
        select as struct 
            split(json_value(performance_levels, "$.assessmentReportingMethodDescriptor"), '#')[OFFSET(1)] as assessment_reporting_method_descriptor,
            split(json_value(performance_levels, "$.performanceLevelDescriptor"), '#')[OFFSET(1)] as performance_level_descriptor,
            cast(json_value(performance_levels, "$.performanceLevelMet") as BOOL) as performance_level_met
        from unnest(json_query_array(data, "$.performanceLevels")) performance_levels 
    ) as performance_levels,
    array(
            select as struct
                struct(
                        json_value(assessments, '$.objectiveAssessmentReference.assessmentIdentifier') as assessment_identifier,
                        json_value(assessments, '$.objectiveAssessmentReference.identificationCode') as identification_code,
                        json_value(assessments, '$.objectiveAssessmentReference.namespace') as namespace
                ) as objective_assessment_reference,
                array(
                    select as struct 
                        split(json_value(performance_levels, "$.assessmentReportingMethodDescriptor"), '#')[OFFSET(1)] as assessment_reporting_method_descriptor,
                        split(json_value(performance_levels, "$.performanceLevelDescriptor"), '#')[OFFSET(1)] as performance_level_descriptor,
                        cast(json_value(performance_levels, "$.performanceLevelMet") as BOOL) as performance_level_met
                    from unnest(json_query_array(assessments, "$.performanceLevels")) performance_levels 
                ) as performance_levels,
                array(
                    select as struct 
                        split(json_value(score_results, "$.assessmentReportingMethodDescriptor"), '#')[OFFSET(1)] as assessment_reporting_method_descriptor,
                        split(json_value(score_results, "$.resultDatatypeTypeDescriptor"), '#')[OFFSET(1)] as result_datatype_type_descriptor,
                        json_value(score_results, '$.result') as result
                    from unnest(json_query_array(assessments, "$.scoreResults")) score_results 
                ) as score_results
            from unnest(json_query_array(data, "$.studentObjectiveAssessments")) assessments
    ) as student_objective_assessments,
from records

{{ remove_edfi_deletes_and_duplicates() }}
