
{{ retrieve_edfi_records_from_data_lake('base_edfi_student_education_organization_associations') }}

select
    date_extracted                          as date_extracted,
    school_year                             as school_year,
    id                                      as id,
    struct(
        json_value(data, '$.educationOrganizationReference.educationOrganizationId') as education_organization_id
    ) as education_organization_reference,
    struct(
        json_value(data, '$.studentReference.studentUniqueId') as student_unique_id
    ) as student_reference,
    array(
        select as struct 
            LOWER(json_value(email, "$.electronicMailAddress")) as address,
            split(json_value(email, "$.electronicMailTypeDescriptor"), '#')[OFFSET(1)] as type_descriptor,
        from unnest(json_query_array(data, "$.electronicMails")) email 
    ) as electronic_mail,
    split(json_value(data, '$.limitedEnglishProficiencyDescriptor'), '#')[OFFSET(1)] as limited_english_proficiency_descriptor,
    cast(json_value(data, '$.hispanicLatinoEthnicity') as BOOL) as hispanic_latino_ethnicity,
    split(json_value(data, '$.sexDescriptor'), '#')[OFFSET(1)] as sex_descriptor,
    array(
        select as struct 
            json_value(student_indicators, "$.indicatorName") as name,
            json_value(student_indicators, "$.designatedBy") as designated_by,
            json_value(student_indicators, "$.indicator") as indicator,
            json_value(student_indicators, "$.indicatorGroup") as indicator_group,
        from unnest(json_query_array(data, "$.studentIndicators")) student_indicators 
    ) as student_indicators,
    array(
        select as struct 
            split(json_value(cohort_years, '$.cohortYearTypeDescriptor'), '#')[OFFSET(1)] as cohort_type_descriptor,
            split(json_value(cohort_years, '$.termDescriptor'), '#')[OFFSET(1)] as term_descriptor,
            json_value(cohort_years, "$.schoolYearTypeReference.schoolYear") as school_year,
        from unnest(json_query_array(data, "$.cohortYears")) cohort_years 
    ) as cohort_years,
    array(
        select as struct 
            split(json_value(disabilities, '$.disabilityDescriptor'), '#')[OFFSET(1)] as disability_descriptor,
            split(json_value(disabilities, '$.disabilityDeterminationSourceTypeDescriptor'), '#')[OFFSET(1)] as disability_determination_source_type_descriptor,
            json_value(disabilities, "$.disabilityDiagnosis") as disability_diagnosis,
        from unnest(json_query_array(data, "$.disabilities")) disabilities 
    ) as disabilities,
    array(
        select as struct 
            split(json_value(languages, '$.languageDescriptor'), '#')[OFFSET(1)] as language_descriptor,
            array(
                select as struct 
                    split(json_value(uses, '$.languageUseDescriptor'), '#')[OFFSET(1)] as language_use_descriptor
                from unnest(json_query_array(languages, '$.uses')) as uses
            ) as uses
        from unnest(json_query_array(data, "$.languages")) languages 
    ) as languages,
    array(
        select as struct 
            split(json_value(races, "$.raceDescriptor"), '#')[OFFSET(1)] as race_descriptor
        from unnest(json_query_array(data, "$.races")) races 
    ) as races,
    array(
        select as struct
            json_value(student_identification_codes, "$.assigningOrganizationIdentificationCode") as assigning_organization_identification_code,
            json_value(student_identification_codes, "$.identificationCode") as identification_code,
            split(json_value(student_identification_codes, "$.studentIdentificationSystemDescriptor"), '#')[OFFSET(1)] as student_identification_system_descriptor
        from unnest(json_query_array(data, "$.studentIdentificationCodes")) student_identification_codes 
    ) as student_identification_codes
from records

{{ remove_edfi_deletes_and_duplicates() }}
