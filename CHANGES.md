# Changelog

# 0.1.0

### New

- Adds `session_name` to `grading_period_key`
- Adds `is_actively_enrolled_in_section` to `fct_student_section_grade`
- Adds `email` to `dim_staff`
- Adds `fct_staff_school`

### Breaking Changes

- Removes `dim_objective_assessment` and moves info to `dim_assessment`
- Removes `dim_demographic`
- Removes `dim_student_program`
- Removes `session_name` from `dim_section`
- Removes `student_section_key` from `fct_student_section_grade`
- Removes `student_local_education_agency_demographics_bridge`
- Moves staff keys out of `dim_section` and into `fct_student_section` and `fct_student_section_grade`
- Removes `rls_user_authorization`
- Removes `rls_student_data_authorization`
- Removed `rls_dim_user`
- Removes `rls_user_school_authorization`


# Previous changelog from combined repo

# v0.5.3

### New

- [dagster] Splits previous warehouse resource into separate data lake and warehouse resources
- [dagster] Updates dagster to version 0.14.1
- [dbt] Adds staff dim
- [dbt] Adds staff section dim
- [dbt] Simplifies logic for `race_and_ethnicity_roll_up` in student dim

### Breaking Changes

- [dbt] Renames `grading_period_description` to `grading_period_name` in `dim_grading_period`
- [dbt] Adds staff to `rpt_student_section_grade` and moves grades to a nested, repeated field



# v0.5.2

### New

- [dagster] Updates dagster to version 0.13.18
- [dagster] Retrieves data from survey endpoints used in [NWEA MAP Dagster workflow](https://github.com/K12-Analytics-Engineering/dagster-nwea-map)
- [dagster] Use of a custom claim set has been added to documentation
- [dagster] Implements use of generator to store JSON as it is retrieved


# v0.5.2

### New

- [dbt] Adds `local_education_agency_key` to `fct_student_attendance`
- [dbt] Adds `local_education_agency_key` to `is_chronically_absent`
- [dbt] Adds `is_chronically_absent` to `fct_student_attendance`
- [dbt] Adds `is_on_the_verge` to `fct_student_attendance`
- [dbt] Removes `reported_as_is_present_in_all_sections` from `fct_student_attendance`
- [dbt] Removes `reported_as_absent_from_any_section` from `fct_student_attendance`
- [dbt] Adds `is_latest_date_avaliable` to `rpt_student_attendance`

### Breaking Changes

- [dbt] Removes `is_chronically_absent` metric in favor of using `fct_student_attendance` table


# v0.5.1

### New

- [dbt] Adds numeric `grade_level_id` columns to `dim_student` for proper sorting in bi layer
- [dbt] Adds `course_gpa_applicability` column to `dim_section`
- [dbt] Adds new unweighted current gpa metric
- [dbt] Adds `student_email` to `dim_student`

### Bugfixes

- [dbt] Fixes dbt data model tagging issue that caused certain tables to not refresh

### Documentation

- [dbt] Documents all macros
- [dbt] Documents core tables

# v0.5.0

### New

- [dbt] Adds `month_sort_order` column to date dim
- [dbt] Updates student attendance fact table to show 'In Attendance' as the attendance event descriptor when no negative attendance exists
- [dbt] Adds user school authorization table for row level security
- [dbt] Adds section dim to core collection
- [dbt] Adds `rpt_student_section_grade` data model
- [dbt] Adds `dim_session`

### Breaking Changes

- [dbt] Fixes bug that flagged all students as early warning for attendance
- [dbt] School year is now an int in all data models
- [dbt] Breaks apart assessment data models into new dimension and fact tables


# v0.4.0

### New

- [dbt] Adds labels noting which data models are a part of Ed-Fi's Analytics Middle Tier
- [dbt] Adds a new `stg_student_attendance` fact table that joins on various dims to add contextual information
- [dbt] Adds student attendance metric for chronically absent and early warning

### Breaking Changes

- [dagster] Adds `base_` prefix to all BigQuery tables created in Dagster job
- [dbt] Updates naming convention of data models to match [bootcamp article](https://github.com/K12-Analytics-Engineering/bootcamp/blob/main/docs/elt_layers.md)
- [dbt] Renames `attendance_fact` table to `student_attendance_fact`


# v0.3.2

### New

- [dbt] Updates `stg_student_assessment_fact` to use nested, repeated fields
- [dbt] Refactors dbt structure to match [bootcamp article](https://github.com/K12-Analytics-Engineering/bootcamp/blob/main/docs/elt_layers.md)
- [dagster] Updates BigQuery permissions to be more restrictive
- [dagster] Updates Dagster to v0.13.14


# v0.3.1

### New

- [dagster] Adds `/programs` to Ed-Fi API extract
- [dagster] Adds `/studentProgramAssociations` to Ed-Fi API extract
- [dagster] Adds `/studentSpecialEducationProgramAssociations` to Ed-Fi API extract
- [dbt] Adds Analytics Middle Tier Assessment and Student Assessment fact tables
- [dbt] Creates native BigQuery table for `/programs` API spec
- [dbt] Creates native BigQuery table for `/studentProgramAssociations` API spec
- [dbt] Creates native BigQuery table for `/studentSpecialEducationProgramAssociations` API spec
- [dbt] Creates `student_dim` view that pre-joins several tables into one combined student dimension
- [dbt] Creates `stg_student_assessment_fact` that joins `assessment_fact` and `student_dim` to provide an easy to use staging fact table


# v0.3.0

### New

- [dagster] Moves Ed-Fi API page limit variable under `edfi_api_client` resource config.
- [dagster] Adds Ed-Fi API mode variable to `edfi_api_client` resource config.
- [dagster] Adds `school_year` as input variable to specify school year scope of data pull.
- [dagster] Updates Google Cloud Storage folder structure to store each school year in its own folder under the `edfi_api` folder.
- [dagster] Updates source URIs in BigQuery external tables to allow for querying multiple school years of data.
- [dbt] Updates dbt SQL to factor in multiple school years of data.
- [dbt] Adds documentation and tests.
