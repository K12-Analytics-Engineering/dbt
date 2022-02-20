
{% macro convert_grade_level_to_short_label(grade_level) %}
    CASE {{ grade_level }}
        WHEN 'Infant/toddler'            THEN 'Infant'
        WHEN 'Preschool/Prekindergarten' THEN 'PreK'
        WHEN 'Transitional Kindergarten' THEN 'TK'
        WHEN 'Kindergarten'              THEN 'K'
        WHEN 'First grade'               THEN '1'
        WHEN 'Second grade'              THEN '2'
        WHEN 'Third grade'               THEN '3'
        WHEN 'Fourth grade'              THEN '4'
        WHEN 'Fifth grade'               THEN '5'
        WHEN 'Sixth grade'               THEN '6'
        WHEN 'Seventh grade'             THEN '7'
        WHEN 'Eighth grade'              THEN '8'
        WHEN 'Ninth grade'               THEN '9'
        WHEN 'Tenth grade'               THEN '10'
        WHEN 'Eleventh grade'            THEN '11'
        WHEN 'Twelfth grade'             THEN '12'
        ELSE '999999999'
    END
{% endmacro %}
