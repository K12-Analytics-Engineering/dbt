
with goals as (

    {% for goal_number in [1,2,3,4] %}

        select distinct 
            test_type                                   as test_type,
            term_name                                   as term_name,
            course                                      as academic_subject,
            "{{goal_number}}"                           as goal_number,
            goal{{goal_number}}_name                    as goal_name
        from {{ ref('stg_nwea_map_assessment_results') }}
        where test_type = 'Survey With Goals' and goal{{goal_number}}_name != ''
        
        {% if not loop.last %} union all {% endif %}

    {% endfor %}

)


select
    goals.goal_name                                                    as IdentificationCode,
    struct(
        concat(
            test_type, "-",
            term_name, "-",
            academic_subject
        )                           as AssessmentIdentifier,
        "uri://nwea.org"            as Namespace
    )                                                                   as AssessmentReference,
    concat("Goal ", goals.goal_number)                                  as Description,
    concat(
        "uri://ed-fi.org/AcademicSubjectDescriptor#",
        goals.academic_subject
    )                                                                   as AcademicSubjectDescriptor,
    array(

        {% for descriptor in ["Low", "LoAvg", "Avg", "HiAvg", "High"] %}

            select as struct
                "uri://ed-fi.org/AssessmentReportingMethodDescriptor#Proficiency level" as AssessmentReportingMethodDescriptor,
                "uri://nwea.org/PerformanceLevelDescriptor#{{descriptor}}"          as PerformanceLevelDescriptor,
                "uri://ed-fi.org/ResultDatatypeTypeDescriptor#Level"                as ResultDatatypeTypeDescriptor
        
                {% if not loop.last %} union all {% endif %}

        {% endfor %}

    )                                                                   as PerformanceLevels,
    array(
        {% for score in [
            ["RIT scale score", "Integer"],
            ["Standard error measurement", "Decimal"]
        ] %}

            select as struct
                "uri://ed-fi.org/AssessmentReportingMethodDescriptor#{{score[0]}}" as AssessmentReportingMethodDescriptor,
                "uri://ed-fi.org/ResultDatatypeTypeDescriptor#{{score[1]}}" as ResultDatatypeTypeDescriptor

            {% if not loop.last %} union all {% endif %}
        
        {% endfor %}

    )                                                                   as Scores
from goals
