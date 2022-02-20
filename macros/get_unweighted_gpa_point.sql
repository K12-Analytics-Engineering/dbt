
{% macro get_unweighted_gpa_point(letter_grade) %}
    CASE {{ letter_grade }}
        WHEN 'A+'   THEN 4.4
        WHEN 'A'    THEN 4
        WHEN 'A-'   THEN 3.7
        WHEN 'B+'   THEN 3.4
        WHEN 'B'    THEN 3
        WHEN 'B-'   THEN 2.7
        WHEN 'C+'   THEN 2.4
        WHEN 'C'    THEN 2
        WHEN 'C-'   THEN 1.7
        WHEN 'D+'   THEN 1.4
        WHEN 'D'    THEN 1.4
        WHEN 'D-'   THEN 0.6
        WHEN 'F'    THEN 0
        ELSE NULL
    END
{% endmacro %}
