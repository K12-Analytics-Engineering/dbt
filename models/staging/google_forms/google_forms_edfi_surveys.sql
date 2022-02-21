
SELECT
    'uri://forms.google.com'        AS namespace,
    form_id                         AS surveyIdentifier,
    STRUCT( 2022 AS schoolYear )    AS schoolYearTypeReference,
    form_title                      AS surveyTitle
FROM {{ ref('stg_google_forms') }}
