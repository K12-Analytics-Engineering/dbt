
{{ retrieve_edfi_records_from_data_lake('base_edfi_staff_school_associations') }}

SELECT
    date_extracted                          AS date_extracted,
    school_year                             AS school_year,
    id                                      AS id,
    STRUCT(
        JSON_VALUE(data, '$.staffReference.staffUniqueId') AS staff_unique_id
    ) AS staff_reference,
    STRUCT(
        JSON_VALUE(data, '$.schoolReference.schoolId') AS school_id
    ) AS school_reference,
    STRUCT(
        CAST(JSON_VALUE(data, '$.schoolYearTypeReference.schoolYear') AS int64) AS school_year
    ) AS school_year_type_reference,
    STRUCT(
        JSON_VALUE(data, '$.calendarReference.calendarCode') AS calendar_code,
        JSON_VALUE(data, '$.calendarReference.schoolId') AS school_id,
        CAST(JSON_VALUE(data, '$.calendarReference.schoolYear') AS int64) AS school_year
    ) AS calendar_reference,
    SPLIT(JSON_VALUE(data, "$.programAssignmentDescriptor"), '#')[OFFSET(1)] AS program_assignment_descriptor,
    ARRAY(
        SELECT AS STRUCT 
            SPLIT(JSON_VALUE(academic_subjects, "$.academicSubjectDescriptor"), '#')[OFFSET(1)] AS academic_subject_descriptor,
        FROM UNNEST(JSON_QUERY_ARRAY(data, "$.academicSubjects")) academic_subjects 
    ) AS academic_subjects,
    ARRAY(
        SELECT AS STRUCT 
            SPLIT(JSON_VALUE(grade_levels, "$.gradeLevelDescriptor"), '#')[OFFSET(1)] AS grade_level_descriptor,
        FROM UNNEST(JSON_QUERY_ARRAY(data, "$.gradeLevels")) grade_levels 
    ) AS grade_levels,
FROM records
WHERE
    extract_type = 'records'
    AND id NOT IN (SELECT id FROM records WHERE extract_type = 'deletes') 
QUALIFY ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY date_extracted DESC) = 1
