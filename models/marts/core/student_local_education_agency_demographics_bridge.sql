{{
  config(
    labels = {'analytics_middle_tier': 'yes'}
  )
}}


WITH demographics AS (
    SELECT
        seoa.school_year                                                            AS school_year,
        {{ dbt_utils.surrogate_key([
            'cohort_years.school_year',
            'cohort_years.cohort_type_descriptor',
            'seoa.student_reference.student_unique_id',
            'seoa.education_organization_reference.education_organization_id'
        ]) }}                                                                       AS student_school_demographic_bridge_key,
        {{ dbt_utils.surrogate_key([
            'seoa.student_reference.student_unique_id',
            'seoa.education_organization_reference.education_organization_id'
        ]) }}                                                                       AS student_local_education_agency_key,
        {{ dbt_utils.surrogate_key([
            'cohort_years.school_year',
            'cohort_years.cohort_type_descriptor'
        ]) }}                                                                       AS demographic_key,
        seoa.education_organization_reference.education_organization_id             AS education_organization_id,
        seoa.student_reference.student_unique_id                                    AS student_unique_id,
        ROW_NUMBER() OVER (
            PARTITION BY
                seoa.student_reference.student_unique_id,
                cohort_years.cohort_type_descriptor
            ORDER BY seoa.school_year DESC
        )                                                                           AS rank
    FROM {{ ref('stg_edfi_student_education_organization_associations') }} seoa
    CROSS JOIN UNNEST(seoa.cohort_years) AS cohort_years

    UNION ALL

    SELECT
        seoa.school_year                                                            AS school_year,
        {{ dbt_utils.surrogate_key([
            'uses.language_use_descriptor', 
            'seoa.student_reference.student_unique_id',
            'seoa.education_organization_reference.education_organization_id'
        ]) }}                                                                       AS student_school_demographic_bridge_key,
        {{ dbt_utils.surrogate_key([
            'seoa.student_reference.student_unique_id',
            'seoa.education_organization_reference.education_organization_id'
        ]) }}                                                                       AS student_local_education_agency_key,
        {{ dbt_utils.surrogate_key([
            'uses.language_use_descriptor'
        ]) }}                                                                       AS demographic_key,
        seoa.education_organization_reference.education_organization_id             AS education_organization_id,
        seoa.student_reference.student_unique_id                                    AS student_unique_id,
        ROW_NUMBER() OVER (
            PARTITION BY
                seoa.student_reference.student_unique_id,
                uses.language_use_descriptor
            ORDER BY seoa.school_year DESC
        )                                                                           AS rank
    FROM {{ ref('stg_edfi_student_education_organization_associations') }} seoa
    CROSS JOIN UNNEST(seoa.languages) AS languages
    CROSS JOIN UNNEST(languages.uses) AS uses

    UNION ALL

    SELECT
        seoa.school_year                                                            AS school_year,
        {{ dbt_utils.surrogate_key([
            'languages.language_descriptor',
            'seoa.student_reference.student_unique_id',
            'seoa.education_organization_reference.education_organization_id'
        ]) }}                                                                       AS student_school_demographic_bridge_key,
        {{ dbt_utils.surrogate_key([
            'seoa.student_reference.student_unique_id',
            'seoa.education_organization_reference.education_organization_id'
        ]) }}                                                                       AS student_local_education_agency_key,
        {{ dbt_utils.surrogate_key([
            'languages.language_descriptor'
        ]) }}                                                                       AS demographic_key,
        seoa.education_organization_reference.education_organization_id             AS education_organization_id,
        seoa.student_reference.student_unique_id                                    AS student_unique_id,
        ROW_NUMBER() OVER (
            PARTITION BY
                seoa.student_reference.student_unique_id,
                languages.language_descriptor
            ORDER BY seoa.school_year DESC
        )                                                                           AS rank
    FROM {{ ref('stg_edfi_student_education_organization_associations') }} seoa
    CROSS JOIN UNNEST(seoa.languages) AS languages

    UNION ALL

    SELECT
        seoa.school_year                                                            AS school_year,
        {{ dbt_utils.surrogate_key([
            'races.race_descriptor', 
            'seoa.student_reference.student_unique_id',
            'seoa.education_organization_reference.education_organization_id'
        ]) }}                                                                       AS student_school_demographic_bridge_key,
        {{ dbt_utils.surrogate_key([
            'seoa.student_reference.student_unique_id',
            'seoa.education_organization_reference.education_organization_id'
        ]) }}                                                                       AS student_local_education_agency_key,
        {{ dbt_utils.surrogate_key([
            'races.race_descriptor'
        ]) }}                                                                       AS demographic_key,
        seoa.education_organization_reference.education_organization_id             AS education_organization_id,
        seoa.student_reference.student_unique_id                                    AS student_unique_id,
        ROW_NUMBER() OVER (
            PARTITION BY
                seoa.student_reference.student_unique_id,
                races.race_descriptor
            ORDER BY seoa.school_year DESC
        )                                                                           AS rank
    FROM {{ ref('stg_edfi_student_education_organization_associations') }} seoa
    CROSS JOIN UNNEST(seoa.races) AS races

)

SELECT
    school_year                                 AS school_year,
    student_school_demographic_bridge_key       AS student_school_demographic_bridge_key,
    student_local_education_agency_key          AS student_local_education_agency_key,
    demographic_key                             AS demographic_key
FROM demographics 
WHERE
    rank = 1
    AND EXISTS (
        SELECT 1
        FROM {{ ref('stg_edfi_student_school_associations') }} ssa
        LEFT JOIN {{ ref('stg_edfi_schools') }} schools
            ON ssa.school_reference.school_id = schools.school_id
            AND ssa.school_year = schools.school_year
        WHERE
            (ssa.exit_withdraw_date IS NULL OR ssa.exit_withdraw_date >= CURRENT_DATE)
            AND schools.local_education_agency_id = demographics.education_organization_id
            AND ssa.student_reference.student_unique_id = demographics.student_unique_id
            AND ssa.school_year = demographics.school_year
    )
