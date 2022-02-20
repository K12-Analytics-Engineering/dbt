{% docs dim_assessment %}

# Assessments dim


---------------------------
Differences from Ed-Fi AMT
* Renames assessment fact table to be a dimension table

{% enddocs %}


{% docs fct_student_assessment %}

# Student assessments fact


---------------------------
Differences from Ed-Fi AMT
* Removes student assessment fact key
* Removes `student_assessment_fact_key`
* Removes `student_objective_assessment_key`
* Adds `assessment_family`
* Adds `school_year`
* Removes `student_school_key`

{% enddocs %}
