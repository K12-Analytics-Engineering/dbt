
{%
    set tables = [{"table": "base_edfi_cohort_type_descriptors", "descriptorId": "cohortTypeDescriptorId"},
    {"table": "base_edfi_disability_descriptors", "descriptorId": "disabilityDescriptorId" },
    {"table": "base_edfi_language_descriptors", "descriptorId": "languageDescriptorId" }, 
    {"table": "base_edfi_language_use_descriptors", "descriptorId": "languageUseDescriptorId" }, 
    {"table": "base_edfi_race_descriptors", "descriptorId": "raceDescriptorId" }, 
    {"table": "base_edfi_grading_period_descriptors", "descriptorId": "gradingPeriodDescriptorId" }]
%}


WITH parsed_data AS (

    {% for table in tables %}
        SELECT
            date_extracted                          AS date_extracted,
            school_year                             AS school_year,
            JSON_VALUE(data, '$.id') AS id,
            JSON_VALUE(data, '$.codeValue') AS code_value,
            JSON_VALUE(data, '{{ "$." ~ table["descriptorId"] }}') AS descriptor_id,
            JSON_VALUE(data, '$.description') AS description,
            JSON_VALUE(data, '$.namespace') AS namespace,
            JSON_VALUE(data, '$.shortDescription') AS short_description
        FROM {{ source('staging', table['table']) }}
        WHERE date_extracted >= (
            SELECT MAX(date_extracted) AS date_extracted
            FROM {{ source('staging', table['table']) }}
            WHERE is_complete_extract IS TRUE)
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY date_extracted DESC) = 1
        {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}

)

SELECT DISTINCT *
FROM parsed_data
WHERE
    id NOT IN (
        SELECT id FROM {{ ref('stg_edfi_deletes') }} edfi_deletes
        WHERE parsed_data.school_year = edfi_deletes.school_year
    )
