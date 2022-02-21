
SELECT
    google_forms_responses.response_id AS surveyResponseIdentifier,
    STRUCT(
        dim_student.student_unique_id AS studentUniqueId
    )   AS studentReference,
    STRUCT(
        'uri://forms.google.com'       AS namespace,
        google_forms_responses.form_id AS surveyIdentifier
    ) AS surveyReference,
    dim_student.email AS electronicMailAddress,
    CONCAT(
        dim_student.student_first_name, ' ',
        dim_student.student_last_surname
     ) AS fullName,
    FORMAT_DATE('%Y-%m-%d', EXTRACT(DATE FROM google_forms_responses.last_submitted)) AS responseDate
FROM {{ ref('stg_google_forms_responses') }} google_forms_responses
LEFT JOIN{{ ref('dim_student') }} dim_student
    ON google_forms_responses.respondent_email = dim_student.email

