# coding: utf-8

from flask import Flask
from flask_cors import CORS, cross_origin
app = Flask(__name__)
CORS(app)
cors = CORS(app, resources={"/search/*": {"origins": "*:*"}})

import sys
import json
import string
from itertools import *
from operator import *

import timeit

from jinjasql import JinjaSql
JSQL = JinjaSql()

from modelYelp import *
from sqlalchemy import text as Text

from nltk.stem.porter import *
stemmer = PorterStemmer()

#avgDL = engine.execute(Text('select avg(review_count) from business;')).first()[0]
#avgDL = float(avgDL)
#avgDLReview = engine.execute(Text('with innerv AS (select review_id, max(index) from index group by review_id limit 30) SELECT avg(max) from innerv;')).first()[0]
#avgDLReview = float(avgDLReview)
avgDL = 53.1536351449
avgDLReview = 118.666666667
k = 1.2 # 1.2 - 2.0
b = 0.75

SQL_RankBusiness = open('SQL/rankBusiness.sql').read()
SQL_RankReview = open('SQL/rankReviews.sql').read()
SQL_Index = open('SQL/index.sql').read()

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

def getReviewSQL(business_ids, avgDL, city, keywords, limit=3):
    sql, binds = JSQL.prepare_query(SQL_RankReview, {
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

def getIndexSQL(keywords, review_ids, city):
    sql, binds = JSQL.prepare_query(SQL_Index, {
        'review_ids': review_ids,
        'keywords': keywords,
        'city': city
    })
    return sql, binds



def search(keywords, city):
    print keywords, city

    START = timeit.default_timer()
    sql, binds = getRankSQL(city, keywords)
    raw = sql % tuple(binds)
    #print raw
    ranks = engine.execute(Text(raw))
    ranks = list(ranks)
    time = timeit.default_timer() - START
    print 'RANK LIST TIME:', time

    START = timeit.default_timer()
    business_ids = map(itemgetter(0), ranks)
    sql, binds = getReviewSQL(business_ids, avgDLReview, city, keywords)
    raw = sql % tuple(binds)
    #print raw
    reviews = engine.execute(Text(raw))

    reviews = list(reviews)
    review_ids = map(itemgetter(2), reviews)
    reviews = {business_id: list(reviews) for business_id, reviews in groupby(reviews, key=itemgetter(1))}
    time = timeit.default_timer() - START
    print 'Review LIST TIME:', time

    START = timeit.default_timer()
    sql, binds = getIndexSQL(keywords, review_ids, city)
    raw = sql % tuple(binds)
    #print raw
    index = list(engine.execute(Text(raw)))
    time = timeit.default_timer() - START
    print 'INDEX TIME:', time

    # this is absolutely unreadable
    index = {review_id: map(itemgetter(2,3,1), idx) for review_id, idx in groupby(index, key=itemgetter(0))}

    _business = []
    _review = {}
    _index = {}
    i = 0
    for rank in ranks:
        i += 1
        business_id, name, stars, review_count, score = rank[:5]
        counts = rank[5:]
        bobj = {
            'name': name,
            'stars': stars,
            'num_reviews': review_count,
            'business_id': business_id,
            'num_keywords': list(zip(map(itemgetter(0), map(itemgetter(0), keywords)), counts))
        }
        '''
        print '-' *55
        print '[%d]' % i, name.encode('utf8'), stars, '/', review_count, '%.4f' % score,
        print stars, zip(map(itemgetter(0), keywords), counts)
        print '-' *55
        '''
        robj = []
        for review in reviews[business_id]:
            row, _business_id, review_id, score, stars, text = review[:6]
            #assert(_business_id == business_id)
            counts = review[6:]
            '''
            print '[%d]' % row, stars, zip(map(itemgetter(0), keywords), counts)
            print text[:1000].encode('utf8')
            print '=='
            '''
            robj.append({
                'text': text,
                'num_keywords': list(zip(map(itemgetter(0), map(itemgetter(0), keywords)), counts)),
                'stars': stars,
                'review_id': review_id
            })
        _review[business_id] = robj
        _business.append(bobj)
    print

    return {'business': _business, 'review': _review, 'index': index, 'keywords': keywords}

@app.route("/search/<city>/<keywords>/<weights>")
@cross_origin(origin='*')
def api_search(city, keywords, weights):
    print keywords
    keywords = keywords.split('|')
    rawKeywords = keywords
    keywords = [map(stemmer.stem, map(string.strip, keyword.split(','))) for keyword in keywords]
    rawKeywords = [map(string.strip, keyword.split(',')) for keyword in rawKeywords]
    weights = map(float, weights.split('|'))

    keywords = list(zip(keywords, weights))
    rawKeywords = list(zip(rawKeywords, weights))

    payload = search(keywords, city)
    print payload['keywords']
    print rawKeywords
    payload['keywords'] = rawKeywords
    return json.dumps(payload)

