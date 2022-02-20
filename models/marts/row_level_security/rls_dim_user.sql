{{
  config(
    labels = {'analytics_middle_tier': 'yes'}
  )
}}


SELECT DISTINCT
    {{ dbt_utils.surrogate_key([
        'staff.staff_unique_id'
     ]) }}                          AS user_key,
    email.electronic_mail_address   AS user_email
FROM {{ ref('stg_edfi_staffs') }} staff,
    UNNEST(staff.electronic_mails) AS email
WHERE email.electronic_mail_type_descriptor = 'Work'
