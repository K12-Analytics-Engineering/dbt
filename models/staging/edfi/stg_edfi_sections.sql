
{{ retrieve_edfi_records_from_data_lake('base_edfi_sections') }}

select
    date_extracted                          as date_extracted,
    school_year                             as school_year,
    id                                      as id,
    json_value(data, '$.sectionIdentifier') as section_identifier,
    json_value(data, '$.sectionName') as section_name,
    struct(
        json_value(data, '$.courseOfferingReference.localCourseCode') as local_course_code,
        json_value(data, '$.courseOfferingReference.schoolId') as school_id,
        cast(json_value(data, '$.courseOfferingReference.schoolYear') as int64) as school_year,
        json_value(data, '$.courseOfferingReference.sessionName') as session_name
    ) as course_offering_reference,
    cast(json_value(data, '$.availableCreditConversion') as float64) as available_credit_conversion,
    cast(json_value(data, '$.availableCredits') as float64) as available_credits,
    split(json_value(data, '$.availableCreditTypeDescriptor'), '#')[OFFSET(1)] as available_credit_type_descriptor,
    split(json_value(data, '$.educationalEnvironmentDescriptor'), '#')[OFFSET(1)] as educational_environment_descriptor,
    struct(
        json_value(data, '$.locationReference.classroomIdentificationCode') as classroom_identification_code,
        json_value(data, '$.locationReference.schoolId') as school_id
    ) as location_reference,
    struct(
        json_value(data, '$.locationSchoolReference.schoolId') as school_id
    ) as location_school_reference,
    array(
        select as struct 
            struct(
                json_value(class_periods, "$.classPeriodReference.classPeriodName") as class_period_name,
                json_value(class_periods, '$.classPeriodReference.schoolId') as school_id
            ) as class_period_reference
        from unnest(json_query_array(data, "$.classPeriods")) class_periods 
    ) as class_periods,
from records

{{ remove_edfi_deletes_and_duplicates() }}
