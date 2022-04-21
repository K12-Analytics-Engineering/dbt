
{% macro get_unweighted_gpa_point(letter_grade) %}
    case {{ letter_grade }}
        when 'A+'   then 4.4
        when 'A'    then 4
        when 'A-'   then 3.7
        when 'B+'   then 3.4
        when 'B'    then 3
        when 'B-'   then 2.7
        when 'C+'   then 2.4
        when 'C'    then 2
        when 'C-'   then 1.7
        when 'D+'   then 1.4
        when 'D'    then 1.4
        when 'D-'   then 0.6
        when 'F'    then 0
        else NULL
    end
{% endmacro %}
