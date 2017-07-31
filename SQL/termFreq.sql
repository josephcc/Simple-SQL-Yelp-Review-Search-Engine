
SELECT 
  token,
  term,
  review,
  business
FROM docFreq
WHERE token in (
  {%- for keyword in keywords %}
      '{{keyword}}'{%- if not loop.last %}, {%- endif %}
  {%- endfor %}
  ) and city = '{{city}}'

