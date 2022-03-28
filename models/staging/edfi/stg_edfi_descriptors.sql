
{{ retrieve_edfi_records_from_data_lake('base_edfi_descriptors') }}

SELECT
    date_extracted                          AS date_extracted,
    school_year                             AS school_year,
    id                                      AS id,
    JSON_VALUE(data, '$.codeValue') AS code_value,
    JSON_VALUE(data, '$.description') AS description,
    JSON_VALUE(data, '$.namespace') AS namespace,
    JSON_VALUE(data, '$.shortDescription') AS short_description
FROM records
WHERE
    extract_type = 'records'
    AND id NOT IN (SELECT id FROM records WHERE extract_type = 'deletes') 
QUALIFY ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY date_extracted DESC) = 1
