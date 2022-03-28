
WITH latest_extract AS (

    SELECT
        school_year,
        MAX(date_extracted) AS date_extracted
    FROM {{ source('staging', 'base_edfi_schools') }}
    WHERE is_complete_extract IS TRUE
    GROUP BY 1

),

records AS (

    SELECT base_table.*
    FROM {{ source('staging', 'base_edfi_schools') }} base_table
    LEFT JOIN latest_extract ON base_table.school_year = latest_extract.school_year
    WHERE
        base_table.date_extracted >= latest_extract.date_extracted
        AND id IS NOT NULL

)


SELECT
    date_extracted                          AS date_extracted,
    school_year                             AS school_year,
    id                                      AS id,
    JSON_VALUE(data, '$.localEducationAgencyReference.localEducationAgencyId') AS local_education_agency_id,
    JSON_VALUE(data, '$.schoolId')          AS school_id,
    JSON_VALUE(data, '$.nameOfInstitution') AS name_of_institution,
    SPLIT(JSON_VALUE(data, '$.schoolTypeDescriptor'), '#')[OFFSET(1)] AS school_type_descriptor,
FROM records
WHERE
    extract_type = 'records'
    AND id NOT IN (SELECT id FROM records WHERE extract_type = 'deletes') 
QUALIFY ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY date_extracted DESC) = 1
