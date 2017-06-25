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
            COUNT(DISTINCT CASE WHEN (
            {%- for token in keyword[0] %}
                token = '{{token}}' {% if not loop.last %} OR {% endif %}
            {%- endfor %}  
            ) THEN review_id ELSE NULL END ) AS {{keyword[0][0]}}{% if not loop.last %},{% endif %}
        {%- endfor %}  
    FROM index WHERE
        ({%- for keyword in keywords %}
            {%- for token in keyword[0] %}
                token = '{{token}}'{% if not loop.last %} OR {% endif %}
            {%- endfor %}{% if not loop.last %} OR {% endif %}
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
        LEAST(1, rank.{{positive[0][0]}}) {% if not loop.last %} + {% endif %}
    {%- endfor %}) / CAST({{positives|length}} AS float) ) *
    ({%- for keyword in keywords %}
        (( {{keyword[1]}} * rank.{{keyword[0][0]}}* ({{k}} + 1)) / 
        (rank.{{keyword[0][0]}} + {{k}} * (1 - {{b}} + ({{b}} * business.review_count / {{avgDL}})))) {% if not loop.last %} + {% endif %}
    {%- endfor %}
    ) AS score,
    {%- for keyword in keywords %}
        rank.{{keyword[0][0]}} {% if not loop.last %},{% endif %}
    {%- endfor %}  
FROM innerView rank LEFT JOIN business business
ON rank.business_id = business.business_id
WHERE business.stars > 2.5 AND business.review_count > 20
ORDER BY score DESC
LIMIT {{limit}};
'''

SQL_RankReviewAll = '''
WITH aggregate AS (
  SELECT
    business_id,
    review_id,
    MAX(index) as document_length,
    {%- for keyword in keywords %}
	SUM(CASE WHEN (
        {%- for token in keyword[0] %}
            token = '{{token}}' {% if not loop.last %} OR {% endif %}
        {%- endfor %}  
        ) THEN 1 ELSE 0 END ) AS {{keyword[0][0]}}{% if not loop.last %},{% endif %}
    {%- endfor %}  
  FROM index WHERE
    ({%- for keyword in keywords %}
        {%- for token in keyword[0] %}
            token = '{{token}}'{% if not loop.last %} OR {% endif %}
        {%- endfor %}{% if not loop.last %} OR {% endif %}
    {%- endfor %}
    ) AND
    city = '{{city}}' AND
    ({%- for business_id in business_ids %}
        business_id = '{{business_id}}'{%- if not loop.last %} OR {%- endif %}
    {%- endfor %})
  GROUP BY business_id, review_id
), rank AS (
  SELECT
    aggregate.business_id,
    aggregate.review_id,

    (({% for positive in positives -%}
        LEAST(1, aggregate.{{positive[0][0]}}) {% if not loop.last %} + {% endif %}
    {%- endfor %}) / CAST({{positives|length}} AS float) ) *
    ({%- for keyword in keywords %}
        ( {{keyword[1]}} * aggregate.{{keyword[0][0]}} ) / GREATEST(1, document_length) {% if not loop.last %} + {% endif %}
    {%- endfor %}
    ) AS score,

    review.text,
      {%- for keyword in keywords %}
          aggregate.{{keyword[0][0]}} {% if not loop.last %},{% endif %}
      {%- endfor %}
  FROM aggregate aggregate LEFT JOIN review review
  ON review.city = '{{city}}' AND aggregate.review_id = review.review_id
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
        {%- for token in keyword[0] %}
            token = '{{token}}'{%- if not loop.last %} OR {%- endif %}
        {%- endfor %}{%- if not loop.last %} OR {%- endif %}
    {%- endfor %}) AND 
    city = '{{city}}'
ORDER BY review_id, token, start;
 '''

def getRankSQL(city, keywords, limit=30):

    keywords = tuple(keywords)
    print keywords
    print {
        'k': k,
        'b': b,
        'avgDL': avgDL,
        'city': city,
        'keywords': keywords,
        'positives': filter(lambda keyword: keyword[1] > 0, keywords),
        'limit': limit
    }

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
        'keywords': keywords,
        'city': city
    })
    
    return sql, binds



def search(keywords, city):

    START = timeit.default_timer()
    sql, binds = getRankSQL(city, keywords)
    raw = sql % tuple(binds)
    print raw
    ranks = engine.execute(Text(raw))
    ranks = list(ranks)
    time = timeit.default_timer() - START
    print 'RANK LIST TIME:', time

    START = timeit.default_timer()
    business_ids = map(itemgetter(0), ranks)
    sql, binds = getReviewAllSQL(business_ids, avgDLReview, city, keywords)
    raw = sql % tuple(binds)
    print raw
    reviews = engine.execute(Text(raw))
    # print reviews.keys() [u'row', u'business_id', u'review_id', u'score', u'text', u'korean', u'vegan', u'street', u'pork']
    reviews = list(reviews)
    review_ids = map(itemgetter(2), reviews)
    reviews = {business_id: list(reviews) for business_id, reviews in groupby(reviews, key=itemgetter(1))}
    time = timeit.default_timer() - START
    print 'Review LIST TIME:', time

    START = timeit.default_timer()
    sql, binds = getIndexSQL(keywords, review_ids, city)
    raw = sql % tuple(binds)
    print raw
    index = list(engine.execute(Text(raw)))
    time = timeit.default_timer() - START
    print 'INDEX TIME:', time
    print list(index)

    i = 0
    for rank in ranks:
        i += 1
        business_id, name, stars, review_count, score = rank[:5]
        counts = rank[5:]
        print '-' *55
        print '[%d]' % i, name.encode('utf8'), stars, '/', review_count, '%.4f' % score,
        print stars, zip(map(itemgetter(0), keywords), counts)
        print '-' *55
        print reviews
        for review in reviews[business_id]:
            row, _business_id, review_id, score, text = review[:5]
            assert(_business_id == business_id)
            counts = review[5:]
            print '[%d]' % row, stars, zip(map(itemgetter(0), keywords), counts)
            print text[:1000].encode('utf8')
            print '=='

import sys
if __name__ == '__main__':
    print sys.argv
    city, stuff = sys.argv[1], sys.argv[2:]
    assert(len(stuff) % 2 ==0)
    keywords = stuff[::2]
    keywords = [map(stemmer.stem, keyword.split(',')) for keyword in keywords]
    keywords = map(set, keywords)
    keywords = map(list, keywords)
    weights = map(float, stuff[1::2])

    keywords = list(zip(keywords, weights))
    keywords = filter(lambda keyword: keyword[1] != 0, keywords)

    print keywords, city
    search(keywords, city)

