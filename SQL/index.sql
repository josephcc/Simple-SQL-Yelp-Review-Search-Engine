SELECT review_id, token, start, "end"
FROM index
WHERE review_id IN
    ({%- for review_id in review_ids %}
        '{{review_id}}'{%- if not loop.last %}, {%- endif %}
    {%- endfor %}) AND token IN
    ({%- for keyword in keywords %}
        {%- for token in keyword[0] %}
            '{{token}}'{%- if not loop.last %}, {%- endif %}
        {%- endfor %}{%- if not loop.last %}, {%- endif %}
    {%- endfor %}) AND 
    city = '{{city}}'
ORDER BY review_id, token, start;
