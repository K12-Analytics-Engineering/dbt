
select
    google_forms_responses.response_id as surveyResponseIdentifier,
    struct(
        dim_student.student_unique_id as studentUniqueId
    )   as studentReference,
    struct(
        'uri://forms.google.com'       as namespace,
        google_forms_responses.form_id as surveyIdentifier
    ) as surveyReference,
    dim_student.email as electronicMailAddress,
    concat(
        dim_student.student_first_name, ' ',
        dim_student.student_last_surname
     ) as fullName,
    FORMAT_DATE('%Y-%m-%d', EXTRACT(DATE from google_forms_responses.last_submitted)) as responseDate
from {{ ref('stg_google_forms_responses') }} google_forms_responses
left join{{ ref('dim_student') }} dim_student
    on google_forms_responses.respondent_email = dim_student.email

