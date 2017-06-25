WITH innterView AS (
    SELECT review_id, business_id, max(index)
    FROM index
    WHERE business_id IN
    ({%- for business_id in business_ids %}
        '{{business_id}}'{%- if not loop.last %}, {%- endif %}
    {%- endfor %})
    GROUP BY review_id, business_id
)
SELECT business_id, avg(max)
FROM innterView 
GROUP BY business_id;
