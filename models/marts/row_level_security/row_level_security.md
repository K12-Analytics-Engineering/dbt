
{% docs rls_user_student_data_authorization %}

# User student data authorization

This row-level security (RLS) table has a grain size of one row per student key. `authorized_emails` is a repeated field containing all user emails who are allowed access to the respective student's data.

If a staff member is actively assigned to a school with a classification of Superintendent, School Administrator, or Principal, they are given access.

If a staff member is actively assigned to a class section where the student has an association, they are given access.

Finally, each student will have their email included for their respective `student_key` record.


{% enddocs %}
