# coding: utf-8

import timeit

from jinjasql import JinjaSql
JSQL = JinjaSql()

from modelYelp import *
from sqlalchemy import text as Text

from itertools import *
from operator import *

from nltk.stem.porter import *
stemmer = PorterStemmer()

#avgDL = engine.execute(Text('select avg(review_count) from business;')).first()[0]
#avgDL = float(avgDL)
avgDL = 53.1536351449
#avgDLReview = engine.execute(Text('with innerv AS (select review_id, max(index) from index group by review_id limit 30) SELECT avg(max) from innerv;')).first()[0]
#avgDLReview = float(avgDLReview)
avgDLReview = 118.666666667
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
    (({% for positive in positives -%}
        LEAST(1, rank.{{positive[0]}}) {% if not loop.last %} + {% endif %}
    {%- endfor %}) / CAST({{positives|length}} AS float) ) *
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
    (({% for positive in positives -%}
        LEAST(1, rank.{{positive[0]}}) {% if not loop.last %} + {% endif %}
    {%- endfor %}) / CAST({{positives|length}} AS float) ) *
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


simplified_score = '''
      (({% for positive in positives -%}
          LEAST(1, aggregate.{{positive[0]}}) {% if not loop.last %} + {% endif %}
      {%- endfor %}) / CAST({{positives|length}} AS float) ) *
      ({%- for keyword in keywords %}
          ( {{keyword[1]}} * aggregate.{{keyword[0]}} ) / GREATEST(1, document_length) {% if not loop.last %} + {% endif %}
      {%- endfor %}
      ) AS score,
'''

bm25_score = '''
    (({% for positive in positives -%}
        LEAST(1, aggregate.{{positive[0]}}) {% if not loop.last %} + {% endif %}
    {%- endfor %}) / CAST({{positives|length}} AS float) ) *
    ({%- for keyword in keywords %}
        (( {{keyword[1]}} * aggregate.{{keyword[0]}}* ({{k}} + 1)) / 
        (aggregate.{{keyword[0]}} + {{k}} * (1 - {{b}} + ({{b}} * document_length / {{avgDL}})))) {% if not loop.last %} + {% endif %}
    {%- endfor %}
    ) AS score,
'''

SQL_RankReviewAll = '''
WITH aggregate AS (
  SELECT
    business_id,
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
    ({%- for business_id in business_ids %}
        business_id = '{{business_id}}'{%- if not loop.last %} OR {%- endif %}
    {%- endfor %})
  GROUP BY business_id, review_id
), rank AS (
  SELECT
    aggregate.business_id,
    aggregate.review_id,

    (({% for positive in positives -%}
        LEAST(1, aggregate.{{positive[0]}}) {% if not loop.last %} + {% endif %}
    {%- endfor %}) / CAST({{positives|length}} AS float) ) *
    ({%- for keyword in keywords %}
        ( {{keyword[1]}} * aggregate.{{keyword[0]}} ) / GREATEST(1, document_length) {% if not loop.last %} + {% endif %}
    {%- endfor %}
    ) AS score,

    review.text,
      {%- for keyword in keywords %}
          aggregate.{{keyword[0]}} {% if not loop.last %},{% endif %}
      {%- endfor %}
  FROM aggregate aggregate LEFT JOIN review review
  ON aggregate.review_id = review.review_id
), label AS (
  SELECT
    ROW_NUMBER() OVER(PARTITION BY rank.business_id ORDER BY rank.score DESC) AS row,
    rank.*
  FROM rank rank
)
SELECT label.*
  FROM label label
WHERE label.row <= {{limit}};
  
'''

SQL_ReviewDL = '''
WITH innterView AS (
    SELECT review_id, business_id, max(index)
    FROM index
    WHERE
    ({%- for business_id in business_ids %}
        business_id = '{{business_id}}'{%- if not loop.last %} OR {%- endif %}
    {%- endfor %})
    GROUP BY review_id, business_id
)
SELECT business_id, avg(max)
FROM innterView 
GROUP BY business_id;
'''

SQL_Index = '''
SELECT review_id, token, start, "end"
FROM index
WHERE
    ({%- for review_id in review_ids %}
        review_id = '{{review_id}}'{%- if not loop.last %} OR {%- endif %}
    {%- endfor %}) AND
    ({%- for keyword in keywords %}
        token = '{{keyword}}'{%- if not loop.last %} OR {%- endif %}
    {%- endfor %}) AND 
    city = '{{city}}'
ORDER BY review_id, token, start;
 '''

def getRankSQL(city, keywords, limit=30):

    sql, binds = JSQL.prepare_query(SQL_RankBusiness, {
        'k': k,
        'b': b,
        'avgDL': avgDL,
        'city': city,
        'keywords': keywords,
        'positives': filter(lambda keyword: keyword[1] > 0, keywords),
        'limit': limit
    })
    
    return sql, binds

def getReviewAllSQL(business_ids, avgDL, city, keywords, limit=3):

    sql, binds = JSQL.prepare_query(SQL_RankReviewAll, {
        'k': k,
        'b': b,
        'avgDL': avgDL,
        'city': city,
        'keywords': keywords,
        'positives': filter(lambda keyword: keyword[1] > 0, keywords),
        'business_ids': business_ids,
        'limit': limit
    })
    
    return sql, binds

def getReviewDLSQL(business_ids):

    ''' # way too slow
    sql, binds = getReviewDLSQL(business_ids)
    reviewDLs = engine.execute(Text(sql % tuple(binds)))
    reviewDLs = {business_id: float(avgdl) for business_id, avgdl in reviewDLs}
    '''

    sql, binds = JSQL.prepare_query(SQL_ReviewDL, {
        'business_ids': business_ids
    })
    
    return sql, binds

def getIndexSQL(keywords, review_ids, city):

    sql, binds = JSQL.prepare_query(SQL_Index, {
        'review_ids': review_ids,
        'keywords': map(itemgetter(0), keywords),
        'city': city
    })
    
    return sql, binds



def search(keywords, city):

    START = timeit.default_timer()
    sql, binds = getRankSQL(city, keywords)
    raw = sql % tuple(binds)

    results = engine.execute(Text(raw))
    results = list(results)
    time = timeit.default_timer() - START
    print 'RANK LIST TIME:', time

    START = timeit.default_timer()
    business_ids = map(itemgetter(0), results)
    sql, binds = getReviewAllSQL(business_ids, avgDLReview, city, keywords)
    raw = sql % tuple(binds)

    reviews = engine.execute(Text(raw))
    # print reviews.keys() [u'row', u'business_id', u'review_id', u'score', u'text', u'korean', u'vegan', u'street', u'pork']
    reviews = list(reviews)
    reviews = {business_id: list(reviews) for business_id, reviews in groupby(reviews, key=itemgetter(1))}
    time = timeit.default_timer() - START
    print 'Review LIST TIME:', time

    i = 0
    review_ids = []
    for result in results:
        i += 1
        business_id, name, stars, review_count, score = result[:5]
        counts = result[5:]
        print '-' *55
        print '[%d]' % i, name.encode('utf8'), stars, '/', review_count, '%.4f' % score,
        print stars, zip(map(itemgetter(0), keywords), counts)
        print '-' *55
        print reviews
        for review in reviews[business_id]:
            row, _business_id, review_id, score, text = review[:5]
            assert(_business_id == business_id)
            counts = review[5:]
            review_ids.append(review_id)
            print '[%d]' % row, stars, zip(map(itemgetter(0), keywords), counts)
            print text[:1000].encode('utf8')
            print '=='

    START = timeit.default_timer()
    sql, binds = getIndexSQL(keywords, review_ids, city)
    raw = sql % tuple(binds)
    results = list(engine.execute(Text(raw)))
    time = timeit.default_timer() - START
    print 'INDEX TIME:', time
    print list(results)
    print raw

import sys
if __name__ == '__main__':
    print sys.argv
    city, stuff = sys.argv[1], sys.argv[2:]
    assert(len(stuff) % 2 ==0)
    keywords = stuff[::2]
    keywords = map(stemmer.stem, keywords)
    weights = map(float, stuff[1::2])

    keywords = list(zip(keywords, weights))
    keywords = filter(lambda keyword: keyword[1] != 0, keywords)

    print keywords, city
    search(keywords, city)

