
SELECT
    {{ dbt_utils.surrogate_key([
            'staff_unique_id',
            'school_year'
    ]) }}                               AS staff_key,
    school_year                         AS school_year,
    staff_unique_id                     AS staff_unique_id,
    last_surname                        AS staff_last_surname,
    middle_name                         AS staff_middle_name,
    first_name                          AS staff_first_name,
    CONCAT(
        last_surname, ', ',
        first_name, ' ',
        COALESCE(LEFT(middle_name, 1), '')
    )                                   AS staff_display_name,
    IF(
        hispanic_latino_ethnicity IS TRUE,
        'Yes',
        'No')                           AS is_hispanic,
    email.electronic_mail_address       AS email
FROM {{ ref('stg_edfi_staffs') }}
LEFT JOIN UNNEST(electronic_mails) email
    ON email.electronic_mail_type_descriptor = 'Work' 
