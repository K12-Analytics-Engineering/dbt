
{% macro convert_grade_level_to_id(grade_level) %}
    case {{ grade_level }}
        when 'Infant/toddler'            then -3
        when 'Preschool/Prekindergarten' then -2
        when 'Transitional Kindergarten' then -1
        when 'Kindergarten'              then 1
        when 'First grade'               then 2
        when 'Second grade'              then 3
        when 'Third grade'               then 4
        when 'Fourth grade'              then 5
        when 'Fifth grade'               then 6
        when 'Sixth grade'               then 7
        when 'Seventh grade'             then 8
        when 'Eighth grade'              then 9
        when 'Ninth grade'               then 10
        when 'Tenth grade'               then 11
        when 'Eleventh grade'            then 12
        when 'Twelfth grade'             then 13
        else 999999999
    end
{% endmacro %}
