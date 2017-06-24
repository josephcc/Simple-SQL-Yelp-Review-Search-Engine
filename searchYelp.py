# coding: utf-8

from jinjasql import JinjaSql
JSQL = JinjaSql()


from modelYelp import *
from sqlalchemy import text as Text
from operator import *

from nltk.stem.porter import *
stemmer = PorterStemmer()


avgDL = engine.execute(Text('select avg(review_count) from business;')).first()[0]
avgDL = float(avgDL)
avgDLReview = engine.execute(Text('with innerv AS (select review_id, max(index) from index group by review_id limit 30) SELECT avg(max) from innerv;')).first()[0]
avgDLReview = float(avgDLReview)
k = 1.2 # 1.2 - 2.0
b = 0.75


## seperate positive / all keywords
SQL_RankBusiness = '''
WITH innerView AS (
    SELECT
        business_id,

        {%- for keyword in keywords %}
            COUNT(DISTINCT CASE WHEN token = '{{keyword[0]}}' then review_id else NULL end ) AS {{keyword[0]}}{% if not loop.last %},{% endif %}
        {%- endfor %}  
    FROM index WHERE
        ({%- for keyword in keywords %}
            token = '{{keyword[0]}}'{% if not loop.last %} OR {% endif %}
        {%- endfor %}
        ) AND
        city = '{{city}}' 
    GROUP BY business_id
)
SELECT
    rank.business_id,
    business.name,
    business.stars,
    business.review_count,
    (({% for keyword in keywords -%}
        LEAST(1, rank.{{keyword[0]}}) {% if not loop.last %} + {% endif %}
    {%- endfor %}) / CAST({{keywords|length}} AS float) ) *
    ({%- for keyword in keywords %}
        (( {{keyword[1]}} * rank.{{keyword[0]}}* ({{k}} + 1)) / 
        (rank.{{keyword[0]}} + {{k}} * (1 - {{b}} + ({{b}} * business.review_count / {{avgDL}})))) {% if not loop.last %} + {% endif %}
    {%- endfor %}
    ) AS score,
    {%- for keyword in keywords %}
        rank.{{keyword[0]}} {% if not loop.last %},{% endif %}
    {%- endfor %}  
FROM innerView rank LEFT JOIN business business
ON rank.business_id = business.business_id
WHERE business.stars > 2.5 AND business.review_count > 20
ORDER BY score DESC
LIMIT {{limit}};
'''

SQL_RankReview = '''
 WITH innerView AS (
  SELECT
    review_id,
    MAX(index) as document_length,
    {%- for keyword in keywords %}
	SUM(CASE WHEN token = '{{keyword[0]}}' then 1 else 0 end ) AS {{keyword[0]}}{% if not loop.last %},{% endif %}
    {%- endfor %}  
  FROM index WHERE
    ({%- for keyword in keywords %}
	token = '{{keyword[0]}}'{% if not loop.last %} OR {% endif %}
    {%- endfor %}
    ) AND
    city = 'Pittsburgh' AND
    business_id = '{{business_id}}'
  GROUP BY review_id
)
SELECT
  rank.review_id,
    (({% for keyword in keywords -%}
        LEAST(1, rank.{{keyword[0]}}) {% if not loop.last %} + {% endif %}
    {%- endfor %}) / CAST({{keywords|length}} AS float) ) *
    ({%- for keyword in keywords %}
        (( {{keyword[1]}} * rank.{{keyword[0]}}* ({{k}} + 1)) / 
        (rank.{{keyword[0]}} + {{k}} * (1 - {{b}} + ({{b}} * document_length / {{avgDL}})))) {% if not loop.last %} + {% endif %}
    {%- endfor %}
    ) AS score,
  review.text,
    {%- for keyword in keywords %}
        rank.{{keyword[0]}} {% if not loop.last %},{% endif %}
    {%- endfor %}  
 FROM innerView rank LEFT JOIN review review
   ON rank.review_id = review.review_id
 ORDER BY score DESC
 LIMIT 3;

'''

def getRankSQL(city, keywords, limit=30):

    sql, binds = JSQL.prepare_query(SQL_RankBusiness, {
        'k': k,
        'b': b,
        'avgDL': avgDL,
        'city': city,
        'keywords': keywords,
        'limit': limit
    })
    
    return sql, binds

def getReviewSQL(business_id, city, keywords, limit=3):

    sql, binds = JSQL.prepare_query(SQL_RankReview, {
        'k': k,
        'b': b,
        'avgDL': avgDLReview,
        'city': city,
        'keywords': keywords,
        'business_id': business_id,
        'limit': limit
    })
    
    return sql, binds


def search(keywords, city):
    sql, binds = getRankSQL(city, keywords)
    raw = sql % tuple(binds)

    results = engine.execute(Text(raw))
    i = 0
    for result in results:
        i += 1
        business_id, name, stars, review_count, score = result[:5]
        counts = result[5:]
        print '-' *55
        print '[%d]' % i, name, stars, '/', review_count, '%.4f' % score,
        print stars, zip(map(itemgetter(0), keywords), counts)

        print '-' *55
        sql, binds = getReviewSQL(business_id, city, keywords)
        raw = sql % tuple(binds)
        reviews = engine.execute(Text(raw))
        for review in reviews:
            review_id, score, text = review[:3]
            counts = review[3:]
            print stars, zip(map(itemgetter(0), keywords), counts)
            print text[:1000].encode('utf8')
            print '=='

import sys
if __name__ == '__main__':
    print sys.argv
    city, stuff = sys.argv[1], sys.argv[2:]
    assert(len(stuff) % 2 ==0)
    keywords = stuff[::2]
    keywords = map(stemmer.stem, keywords)
    weights = map(float, stuff[1::2])

    print list(zip(keywords, weights)), city

    search(list(zip(keywords, weights)), city)

