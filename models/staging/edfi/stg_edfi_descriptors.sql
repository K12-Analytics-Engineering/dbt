
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
            CAST(JSON_VALUE(data, '$.extractedTimestamp') AS TIMESTAMP) AS extracted_timestamp,
            JSON_VALUE(data, '$.id') AS id,
            CAST(JSON_VALUE(data, '$.schoolYear') AS int64) school_year,
            JSON_VALUE(data, '$.codeValue') AS code_value,
            JSON_VALUE(data, '{{ "$." ~ table["descriptorId"] }}') AS descriptor_id,
            JSON_VALUE(data, '$.description') AS description,
            JSON_VALUE(data, '$.namespace') AS namespace,
            JSON_VALUE(data, '$.shortDescription') AS short_description
        FROM {{ source('staging', table['table']) }}
        {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}

),

ranked AS (

    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY
                school_year,
                namespace,
                code_value
            ORDER BY school_year DESC, extracted_timestamp DESC
        ) AS rank,
        *
    FROM parsed_data

)

SELECT DISTINCT * EXCEPT (extracted_timestamp, rank)
FROM ranked
WHERE
    rank = 1
    AND id NOT IN (
        SELECT id FROM {{ ref('stg_edfi_deletes') }} edfi_deletes
        WHERE ranked.school_year = edfi_deletes.school_year
    )
