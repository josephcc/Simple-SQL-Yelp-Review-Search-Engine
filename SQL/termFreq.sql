
SELECT 
  token,
  COUNT(*),
  COUNT(DISTINCT review_id),
  COUNT(DISTINCT business_id)
FROM index
WHERE token in (
  {%- for keyword in keywords %}
      '{{keyword}}'{%- if not loop.last %}, {%- endif %}
  {%- endfor %}
  ) and city = '{{city}}'
GROUP BY token;

