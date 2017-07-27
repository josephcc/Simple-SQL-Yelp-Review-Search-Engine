SELECT name, stars, review_count FROM business WHERE business_id = '{{business_id}}' AND city = '{{city}}' LIMIT 1;
