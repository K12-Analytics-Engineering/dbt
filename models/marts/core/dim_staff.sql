
select
    {{ dbt_utils.surrogate_key([
            'staff_unique_id',
            'school_year'
    ]) }}                               as staff_key,
    school_year                         as school_year,
    staff_unique_id                     as staff_unique_id,
    last_surname                        as staff_last_surname,
    middle_name                         as staff_middle_name,
    first_name                          as staff_first_name,
    concat(
        last_surname, ', ',
        first_name, ' ',
        COALESCE(LEFT(middle_name, 1), '')
    )                                          as staff_display_name,
    if(
        hispanic_latino_ethnicity is true,
        'Yes',
        'No')                                  as is_hispanic,
    LOWER(email.electronic_mail_address)       as email
from {{ ref('stg_edfi_staffs') }}
left join unnest(electronic_mails) email
    on email.electronic_mail_type_descriptor = 'Work'
