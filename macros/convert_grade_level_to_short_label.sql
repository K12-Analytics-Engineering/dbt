
{% macro convert_grade_level_to_short_label(grade_level) %}
    case {{ grade_level }}
        when 'Infant/toddler'            then 'Infant'
        when 'Preschool/Prekindergarten' then 'PreK'
        when 'Transitional Kindergarten' then 'TK'
        when 'Kindergarten'              then 'K'
        when 'First grade'               then '1'
        when 'Second grade'              then '2'
        when 'Third grade'               then '3'
        when 'Fourth grade'              then '4'
        when 'Fifth grade'               then '5'
        when 'Sixth grade'               then '6'
        when 'Seventh grade'             then '7'
        when 'Eighth grade'              then '8'
        when 'Ninth grade'               then '9'
        when 'Tenth grade'               then '10'
        when 'Eleventh grade'            then '11'
        when 'Twelfth grade'             then '12'
        else '999999999'
    end
{% endmacro %}
