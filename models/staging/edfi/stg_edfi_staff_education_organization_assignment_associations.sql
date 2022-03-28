
WITH latest_extract AS (

    SELECT
        school_year,
        MAX(date_extracted) AS date_extracted
    FROM {{ source('staging', 'base_edfi_staff_education_organization_assignment_associations') }}
    WHERE is_complete_extract IS TRUE
    GROUP BY 1

),

records AS (

    SELECT base_table.*
    FROM {{ source('staging', 'base_edfi_staff_education_organization_assignment_associations') }} base_table
    LEFT JOIN latest_extract ON base_table.school_year = latest_extract.school_year
    WHERE
        base_table.date_extracted >= latest_extract.date_extracted
        AND id IS NOT NULL

)


SELECT
    date_extracted                          AS date_extracted,
    school_year                             AS school_year,
    id                                      AS id,
    STRUCT(
        JSON_VALUE(data, '$.staffReference.staffUniqueId') AS staff_unique_id
    ) AS staff_reference,
    SPLIT(JSON_VALUE(data, "$.staffClassificationDescriptor"), '#')[OFFSET(1)] AS staff_classification_descriptor,
    STRUCT(
        JSON_VALUE(data, '$.educationOrganizationReference.educationOrganizationId') AS education_organization_id
    ) AS education_organization_reference,
    PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, '$.beginDate')) AS begin_date,
    PARSE_DATE('%Y-%m-%d', JSON_VALUE(data, '$.endDate')) AS end_date
FROM records
WHERE
    extract_type = 'records'
    AND id NOT IN (SELECT id FROM records WHERE extract_type = 'deletes') 
QUALIFY ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY date_extracted DESC) = 1
