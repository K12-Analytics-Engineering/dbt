
WITH emails AS (

    SELECT
        stg_edfi_staffs.staff_unique_id AS staff_unique_id,
        emails.electronic_mail_address  AS electronic_mail_address
    FROM {{ ref('stg_edfi_staffs') }} stg_edfi_staffs
    CROSS JOIN UNNEST(electronic_mails) AS emails
    WHERE emails.electronic_mail_type_descriptor = 'Work'

)


SELECT
    school_year_type_reference.school_year          AS school_year,
    {{ dbt_utils.surrogate_key([
        'staff_reference.staff_unique_id' 
    ]) }}                                           AS user_key,
    {{ dbt_utils.surrogate_key([
        'school_reference.school_id'
    ]) }}                                           AS school_key,
    emails.electronic_mail_address                  AS email
FROM {{ ref('stg_edfi_staff_school_associations') }} stg_edfi_staff_school_associations
LEFT JOIN emails
    ON stg_edfi_staff_school_associations.staff_reference.staff_unique_id = emails.staff_unique_id
