
{{ retrieve_edfi_records_from_data_lake('base_edfi_school_year_types') }}

SELECT DISTINCT
    date_extracted                                  AS date_extracted,
    CAST(JSON_VALUE(data, '$.schoolYear') AS int64) AS school_year,
    id                                      AS id,
    JSON_VALUE(data, '$.schoolYearDescription')     AS school_year_description
FROM records

{{ remove_edfi_deletes_and_duplicates() }}
