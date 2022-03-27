
WITH parsed_data AS (

    SELECT
        date_extracted                          AS date_extracted,
        school_year                             AS school_year,
        JSON_VALUE(data, '$.id') AS id,
        STRUCT(
            JSON_VALUE(data, '$.staffReference.staffUniqueId') AS staff_unique_id
        ) AS staff_reference,
        SPLIT(JSON_VALUE(data, "$.staffClassificationDescriptor"), '#')[OFFSET(1)] AS staff_classification_descriptor,
        STRUCT(
            JSON_VALUE(data, '$.educationOrganizationReference.educationOrganizationId') AS education_organization_id
        ) AS education_organization_reference,
        PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, '$.beginDate')) AS begin_date,
        PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, '$.endDate')) AS end_date
    FROM {{ source('staging', 'base_edfi_staff_education_organization_assignment_associations') }}
    WHERE date_extracted >= (
        SELECT MAX(date_extracted) AS date_extracted
        FROM {{ source('staging', 'base_edfi_staff_education_organization_assignment_associations') }}
        WHERE is_complete_extract IS TRUE)
    QUALIFY ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY date_extracted DESC) = 1

)


SELECT *
FROM parsed_data
WHERE
    id NOT IN (
        SELECT id FROM {{ ref('stg_edfi_deletes') }} edfi_deletes
        WHERE parsed_data.school_year = edfi_deletes.school_year)
