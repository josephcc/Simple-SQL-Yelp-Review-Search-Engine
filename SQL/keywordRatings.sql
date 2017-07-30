SELECT
  index.token,
  index.business_id,
  AVG(stars),
  STDDEV(stars),
  COUNT(DISTINCT index.review_id)
FROM
    index LEFT JOIN review ON index.review_id = review.review_id
WHERE
    index.city = '{{city}}' AND
    index.business_id IN (
        {%- for business_id in business_ids %}
          '{{business_id}}'{% if not loop.last %}, {% endif %}
        {%- endfor %}
        ) AND
    token IN (
        {%- for keyword in keywords %}
          '{{keyword}}'{% if not loop.last %}, {% endif %}
        {%- endfor %}
        )
GROUP BY index.token, index.business_id;
