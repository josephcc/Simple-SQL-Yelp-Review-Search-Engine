WITH innerView AS (
    SELECT
        business_id,
        {%- for keyword in keywords %}
            COUNT(DISTINCT CASE WHEN token IN (
            {%- for token in keyword[0] %}
                '{{token}}'{% if not loop.last %}, {% endif %}
            {%- endfor %}  
            ) THEN review_id ELSE NULL END ) AS {{keyword[0][0]}}{% if not loop.last %},{% endif %}
        {%- endfor %}  
    FROM index WHERE
        city = '{{city}}' AND
        token IN (
            {%- for keyword in keywords %}
                {%- for token in keyword[0] %}
                    '{{token}}'{% if not loop.last %}, {% endif %}
                {%- endfor %}{% if not loop.last %}, {% endif %}
            {%- endfor %}
        )
    GROUP BY business_id
)
SELECT
    rank.business_id,
    business.name,
    business.stars,
    business.review_count,
    ST_AsGeoJSON(business.location),
    (({% for positive in positives -%}
        LEAST(1, rank.{{positive[0][0]}}) {% if not loop.last %} + {% endif %}
    {%- endfor %}) / CAST({{positives|length}} AS float) ) *
    ({%- for keyword in keywords %}
        (( {{keyword[1]}} * rank.{{keyword[0][0]}}* ({{k}} + 1)) / 
        (rank.{{keyword[0][0]}} + {{k}} * (1 - {{b}} + ({{b}} * business.review_count / {{avgDL}})))) {% if not loop.last %} + {% endif %}
    {%- endfor %}
    ) AS score,
    {%- for keyword in keywords %}
        rank.{{keyword[0][0]}} {% if not loop.last %},{% endif %}
    {%- endfor %}  
FROM innerView rank LEFT JOIN business business
ON rank.business_id = business.business_id
WHERE business.stars >= {{stars}} AND business.review_count > 15
ORDER BY score DESC
LIMIT {{limit}};
