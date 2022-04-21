
{{ retrieve_edfi_records_from_data_lake('base_edfi_grades') }}

select
    date_extracted                          as date_extracted,
    school_year                             as school_year,
    id                                      as id,
    cast(json_value(data, '$.numericGradeEarned') as float64) as numeric_grade_earned,
    json_value(data, '$.letterGradeEarned') as letter_grade_earned,
    split(json_value(data, '$.performanceBaseConversionDescriptor'), '#')[OFFSET(1)] as performance_base_conversion_descriptor, 
    split(json_value(data, '$.gradeTypeDescriptor'), '#')[OFFSET(1)] as grade_type_descriptor, 
    json_value(data, '$.diagnosticStatement') as diagnostic_statement,
    struct(
        split(json_value(data, '$.gradingPeriodReference.gradingPeriodDescriptor'), '#')[OFFSET(1)] as grading_period_descriptor,
        cast(json_value(data, '$.gradingPeriodReference.periodSequence') as int64) as period_sequence,
        json_value(data, '$.gradingPeriodReference.schoolId') as school_id,
        cast(json_value(data, '$.gradingPeriodReference.schoolYear') as int64) as school_year
    ) as grading_period_reference,
    struct(
        EXTRACT(DATE from PARSE_TIMESTAMP('%Y-%m-%dT%TZ', json_value(data, '$.studentSectionAssociationReference.beginDate'))) as begin_date,
        json_value(data, '$.studentSectionAssociationReference.localCourseCode') as local_course_code,
        json_value(data, '$.studentSectionAssociationReference.schoolId') as school_id,
        cast(json_value(data, '$.studentSectionAssociationReference.schoolYear') as int64) as school_year,
        json_value(data, '$.studentSectionAssociationReference.sectionIdentifier') as section_identifier,
        json_value(data, '$.studentSectionAssociationReference.sessionName') as session_name,
        json_value(data, '$.studentSectionAssociationReference.studentUniqueId') as student_unique_id
    ) as student_section_association_reference
from records

{{ remove_edfi_deletes_and_duplicates() }}
