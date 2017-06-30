SELECT business_id, STRING_AGG(name, '|')
FROM category
WHERE business_id IN (
  {%- for business_id in business_ids %}
      '{{business_id}}'{%- if not loop.last %}, {%- endif %}
  {%- endfor %}
)
GROUP BY business_id;
