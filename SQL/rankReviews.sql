WITH aggregate AS (
  SELECT
    business_id,
    review_id,
    MAX(index) as document_length,
    {%- for keyword in keywords %}
	SUM(CASE WHEN token IN (
        {%- for token in keyword[0] %}
            '{{token}}'{%- if not loop.last %}, {%- endif %}
        {%- endfor %}  
        ) THEN 1 ELSE 0 END ) AS {{keyword[0][0]}}{% if not loop.last %},{% endif %}
    {%- endfor %}  
  FROM index WHERE token IN
    ({%- for keyword in keywords %}
        {%- for token in keyword[0] %}
            '{{token}}'{% if not loop.last %}, {% endif %}
        {%- endfor %}{% if not loop.last %}, {% endif %}
    {%- endfor %}
    ) AND
    city = '{{city}}' AND business_id IN
    ({%- for business_id in business_ids %}
        '{{business_id}}'{%- if not loop.last %}, {%- endif %}
    {%- endfor %})
  GROUP BY business_id, review_id
), rank AS (
  SELECT
    aggregate.business_id,
    aggregate.review_id,

    (({% for positive in positives -%}
        LEAST(1, aggregate.{{positive[0][0]}}) {% if not loop.last %} + {% endif %}
    {%- endfor %}) / CAST({{positives|length}} AS float) ) *
    ({%- for keyword in keywords %}
        ( {{keyword[1]}} * aggregate.{{keyword[0][0]}} ) / GREATEST(1, document_length) {% if not loop.last %} + {% endif %}
    {%- endfor %}
    ) AS score,
    review.stars,
    document_length,
    review.text,
      {%- for keyword in keywords %}
          aggregate.{{keyword[0][0]}} {% if not loop.last %},{% endif %}
      {%- endfor %}
  FROM aggregate aggregate LEFT JOIN review review
  ON review.city = '{{city}}' AND aggregate.review_id = review.review_id
), label AS (
  SELECT
    ROW_NUMBER() OVER(PARTITION BY rank.business_id ORDER BY rank.score DESC) AS row,
    rank.*
  FROM rank rank
)
SELECT label.*
  FROM label label
WHERE label.row <= {{limit}};
