SELECT review_id, stars, text FROM review WHERE city = '{{city}}' AND business_id = '{{business_id}}' LIMIT {{limit}};
