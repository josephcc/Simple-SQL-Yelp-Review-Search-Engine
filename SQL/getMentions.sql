{%- for keyword in keywords %}
  (SELECT
    index.token,
    index.business_id,
    index.review_id,
    SUBSTRING(review.text, index.start - {{context}}, {{context}}),
    SUBSTRING(review.text, index.start, index.end - index.start + 1),
    SUBSTRING(review.text, index.end+1, {{context}})
    FROM index index LEFT JOIN review review ON index.review_id = review.review_id
    WHERE review.city = '{{city}}' AND index.city = '{{city}}' AND token = '{{keyword}}'
    LIMIT {{limitPerKeyword}}
  ) {% if not loop.last %} UNION {% endif %}
{%- endfor %}
