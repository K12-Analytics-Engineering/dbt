
WITH goals AS (

    {% for goal_number in [1,2,3,4] %}

        SELECT DISTINCT 
            test_type                                   AS test_type,
            term_name                                   AS term_name,
            course                                      AS academic_subject,
            "{{goal_number}}"                           AS goal_number,
            goal{{goal_number}}_name                    AS goal_name
        FROM {{ ref('stg_nwea_map_assessment_results') }}
        WHERE test_type = 'Survey With Goals' AND goal{{goal_number}}_name != ''
        
        {% if not loop.last %} UNION ALL {% endif %}

    {% endfor %}

)


SELECT
    goals.goal_name                                                    AS IdentificationCode,
    STRUCT(
        CONCAT(
            test_type, "-",
            term_name, "-",
            academic_subject
        )                           AS AssessmentIdentifier,
        "uri://nwea.org"            AS Namespace
    )                                                                   AS AssessmentReference,
    CONCAT("Goal ", goals.goal_number)                                  AS Description,
    CONCAT(
        "uri://ed-fi.org/AcademicSubjectDescriptor#",
        goals.academic_subject
    )                                                                   AS AcademicSubjectDescriptor,
    ARRAY(

        {% for descriptor in ["Low", "LoAvg", "Avg", "HiAvg", "High"] %}

            SELECT AS STRUCT
                "uri://ed-fi.org/AssessmentReportingMethodDescriptor#Proficiency level" AS AssessmentReportingMethodDescriptor,
                "uri://nwea.org/PerformanceLevelDescriptor#{{descriptor}}"          AS PerformanceLevelDescriptor,
                "uri://ed-fi.org/ResultDatatypeTypeDescriptor#Level"                AS ResultDatatypeTypeDescriptor
        
                {% if not loop.last %} UNION ALL {% endif %}

        {% endfor %}

    )                                                                   AS PerformanceLevels,
    ARRAY(
        {% for score in [
            ["RIT scale score", "Integer"],
            ["Standard error measurement", "Decimal"]
        ] %}

            SELECT AS STRUCT
                "uri://ed-fi.org/AssessmentReportingMethodDescriptor#{{score[0]}}" AS AssessmentReportingMethodDescriptor,
                "uri://ed-fi.org/ResultDatatypeTypeDescriptor#{{score[1]}}" AS ResultDatatypeTypeDescriptor

            {% if not loop.last %} UNION ALL {% endif %}
        
        {% endfor %}

    )                                                                   AS Scores
FROM goals
