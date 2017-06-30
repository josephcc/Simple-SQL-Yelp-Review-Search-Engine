SELECT business_id, name FROM business WHERE name ILIKE '%%{{name}}%%' AND city = '{{city}}' LIMIT {{N}};
