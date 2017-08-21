
SELECT 
  token,
  term,
  review,
  business
FROM docFreq
WHERE city = '{{city}}' AND token IN (
  {%- for keyword in keywords %}
      '{{keyword}}'{%- if not loop.last %}, {%- endif %}
  {%- endfor %}
  )

