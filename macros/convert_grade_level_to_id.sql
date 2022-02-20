
{% macro convert_grade_level_to_id(grade_level) %}
    CASE {{ grade_level }}
        WHEN 'Infant/toddler'            THEN -3
        WHEN 'Preschool/Prekindergarten' THEN -2
        WHEN 'Transitional Kindergarten' THEN -1
        WHEN 'Kindergarten'              THEN 1
        WHEN 'First grade'               THEN 2
        WHEN 'Second grade'              THEN 3
        WHEN 'Third grade'               THEN 4
        WHEN 'Fourth grade'              THEN 5
        WHEN 'Fifth grade'               THEN 6
        WHEN 'Sixth grade'               THEN 7
        WHEN 'Seventh grade'             THEN 8
        WHEN 'Eighth grade'              THEN 9
        WHEN 'Ninth grade'               THEN 10
        WHEN 'Tenth grade'               THEN 11
        WHEN 'Eleventh grade'            THEN 12
        WHEN 'Twelfth grade'             THEN 13
        ELSE 999999999
    END
{% endmacro %}
