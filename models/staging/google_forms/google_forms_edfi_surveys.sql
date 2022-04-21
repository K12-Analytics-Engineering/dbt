
select
    'uri://forms.google.com'        as namespace,
    form_id                         as surveyIdentifier,
    struct( 2022 as schoolYear )    as schoolYearTypeReference,
    form_title                      as surveyTitle
from {{ ref('stg_google_forms') }}
