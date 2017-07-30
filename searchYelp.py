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

from gensim.models.doc2vec import Doc2Vec

START = timeit.default_timer()
model = Doc2Vec.load('./models/yelp.model')
time = timeit.default_timer() - START
print 'MODEL LOAD TIME:', time

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

numberOfBusinesses = dict(list(engine.execute(Text(r'''SELECT city, COUNT(business_id) FROM business GROUP BY city HAVING COUNT(business_id) > 1000;'''))))

SQL_RankBusiness = open('SQL/rankBusiness.sql').read()
SQL_RankReview = open('SQL/rankReviews.sql').read()
SQL_GetReview = open('SQL/getReviews.sql').read()
SQL_Index = open('SQL/index.sql').read()
SQL_TermFreq = open('./SQL/termFreq.sql').read()
SQL_Business = open('./SQL/business.sql').read()
SQL_Category = open('./SQL/category.sql').read()
SQL_KeywordRatings = open('./SQL/keywordRatings.sql').read()

def getRankSQL(city, keywords, stars, limit=20):
    sql, binds = JSQL.prepare_query(SQL_RankBusiness, {
        'k': k,
        'b': b,
        'avgDL': avgDL,
        'city': city,
        'stars': stars,
        'keywords': keywords,
        'positives': filter(lambda keyword: keyword[1] > 0, keywords),
        'limit': limit
    })
    return sql, binds

def getKeywordRatingsSQL(keywords, business_ids, city):
    sql, binds = JSQL.prepare_query(SQL_KeywordRatings, {
        'city': city,
        'business_ids': business_ids,
        'keywords': keywords
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

def getBusinessReviewSQL(business_id, city, limit=20):
    sql, binds = JSQL.prepare_query(SQL_GetReview, {
        'city': city,
        'business_id': business_id,
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

def getCategorySQL(business_ids):
    sql, binds = JSQL.prepare_query(SQL_Category, {
        'business_ids': business_ids
    })
    return sql, binds



def search(keywords, city, stars, allKeywords):
    print keywords, city

    START = timeit.default_timer()
    sql, binds = getRankSQL(city, keywords, stars)
    raw = sql % tuple(binds)
    #print raw
    ranks = engine.execute(Text(raw))
    ranks = list(ranks)
    time = timeit.default_timer() - START
    print 'RANK LIST TIME:', time

    START = timeit.default_timer()
    business_ids = map(itemgetter(0), ranks)
    sql, binds = getReviewSQL(business_ids, avgDLReview, city, keywords, limit=2)
    raw = sql % tuple(binds)
    #print raw
    reviews = engine.execute(Text(raw))

    reviews = list(reviews)
    review_ids = map(itemgetter(2), reviews)
    reviews = {business_id: list(reviews) for business_id, reviews in groupby(reviews, key=itemgetter(1))}
    time = timeit.default_timer() - START
    print 'Review LIST TIME:', time

    START = timeit.default_timer()
    _keywords = map(itemgetter(0), map(itemgetter(0), keywords))
    sql, binds = getKeywordRatingsSQL(_keywords, business_ids, city)
    raw = sql % tuple(binds)
    results = engine.execute(Text(raw))
    # TODO this is absolutely unreadable
    keywordRatings = {business_id: sorted(map(itemgetter(0, 2, 3, 4), items), key=itemgetter(0)) for business_id, items in groupby(sorted(results, key=itemgetter(1)), itemgetter(1))}
    time = timeit.default_timer() - START
    print 'KWRatings LIST TIME:', time

    '''
    START = timeit.default_timer()
    sql, binds = getCategorySQL(business_ids)
    raw = sql % tuple(binds)
    #print raw
    results = engine.execute(Text(raw))
    categories = {business_id: categories.split('|') for business_id, categories in results}
    time = timeit.default_timer() - START
    print 'Category LIST TIME:', time
    '''

    START = timeit.default_timer()
    sql, binds = getIndexSQL(keywords, review_ids, city)
    raw = sql % tuple(binds)
    #print raw
    index = list(engine.execute(Text(raw)))
    time = timeit.default_timer() - START
    print 'INDEX TIME:', time


    START = timeit.default_timer()

    _keywords = map(itemgetter(0), map(itemgetter(0), allKeywords))
    sql, binds = JSQL.prepare_query(SQL_TermFreq, {'city': city, 'keywords': _keywords})
    raw = sql % tuple(binds)
    stats = engine.execute(Text(raw))
    stats = {item[0]: item[-1] for item in stats}
    stats['__numberOfBusinesses__'] = numberOfBusinesses[city]
    print stats

    time = timeit.default_timer() - START
    print 'STATS TIME:', time

    # this is absolutely unreadable
    index = {review_id: map(itemgetter(2,3,1), idx) for review_id, idx in groupby(index, key=itemgetter(0))}

    _business = []
    _review = {}
    i = 0
    _counts = {keyword[0]: 0 for keyword in map(itemgetter(0), keywords)}
    for rank in ranks:
        i += 1
        business_id, name, stars, review_count, score = rank[:5]
        counts = rank[5:]
        bobj = {
            'name': name,
            'stars': stars,
            'num_reviews': review_count,
            'business_id': business_id,
            #'categories': [],
            'num_keywords': list(zip(map(itemgetter(0), map(itemgetter(0), keywords)), counts))
        }
        for idx in range(len(counts)):
            if counts[idx] > 0:
                _counts[keywords[idx][0][0]] += 1

        '''
        if business_id in categories:
            bobj['categories'] = categories[business_id]

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

    return {'business': _business, 'review': _review, 'index': index, 'keywords': keywords, 'counts': _counts, 'keywordRatings': keywordRatings, 'stats': stats}

@app.route("/search/<city>/<stars>/<keywords>/<weights>")
@cross_origin(origin='*')
def api_search(city, stars, keywords, weights):
    print keywords
    keywords = keywords.split('|')
    rawKeywords = keywords
    keywords = [map(stemmer.stem, map(string.strip, keyword.split(','))) for keyword in keywords]
    rawKeywords = [map(string.strip, keyword.split(',')) for keyword in rawKeywords]
    weights = map(float, weights.split('|'))

    keywords = list(zip(keywords, weights))
    allKeywords = keywords[:]
    keywords = filter(lambda k: k[1] != 0, keywords)
    rawKeywords = list(zip(rawKeywords, weights))

    payload = search(keywords, city, stars, allKeywords)
    #payload['keywords'] = rawKeywords
    return json.dumps(payload)

@app.route("/expand/<city>/<keywords>")
@cross_origin(origin='*')
def api_expand(city, keywords):
    keywords = map(string.strip, keywords.split(','))
    print keywords
    candidates = model.most_similar(keywords, topn=100)
    seen = set(map(stemmer.stem, keywords))
    out = []
    for candidate, score in candidates:
        if stemmer.stem(candidate) not in seen:
            out.append([candidate, score])
            seen.add(stemmer.stem(candidate))

    sql, binds = JSQL.prepare_query(SQL_TermFreq, {'city': city, 'keywords': map(stemmer.stem, map(itemgetter(0), out))})
    raw = sql % tuple(binds)
    terms = engine.execute(Text(raw))
    terms = {term: list(counts)[0] for term, counts in groupby(list(terms), key=itemgetter(0))}

    out2 = []
    for term, score in out:
        stem = stemmer.stem(term)
        if stem in terms:
            _, count, review_count, business_count = terms[stem]
        else:
            count, review_count, business_count = 0, 0, 0

        if count == 0:
            continue
        out2.append({'keyword': term, 'score': score, 'count': count, 'review_count': review_count, 'business_count': business_count})

    return json.dumps(out2)


@app.route("/business/<city>/<keyword>")
@cross_origin(origin='*')
def api_business(city, keyword):
    print keyword
    sql, binds = JSQL.prepare_query(SQL_Business, {'city': city, 'name': keyword, 'N': 5})
    print sql
    print binds
    raw = sql % tuple(binds)
    business = engine.execute(Text(raw))
    business = list(business)

    out = []
    for business_id, name in business:
        out.append({'business_id': business_id, 'name': name})

    return json.dumps(out)

@app.route("/reviews/<business_id>/<city>/<keywords>/<weights>")
@cross_origin(origin='*')
def api_reviews(business_id, city, keywords, weights):
    print keywords
    keywords = keywords.split('|')
    rawKeywords = keywords
    keywords = [map(stemmer.stem, map(string.strip, keyword.split(','))) for keyword in keywords]
    weights = map(float, weights.split('|'))
    keywords = list(zip(keywords, weights))


    START = timeit.default_timer()
    sql, binds = getReviewSQL([business_id], avgDLReview, city, keywords, limit=30)
    raw = sql % tuple(binds)
    #print raw
    reviews = engine.execute(Text(raw))
    #[u'row', u'business_id', u'review_id', u'score', u'stars', u'text', u'relax', u'thai', u'noodl']
    reviews = list(reviews)
    review_ids = map(itemgetter(2), reviews)
    time = timeit.default_timer() - START
    print 'Review LIST TIME:', time

    _review = []
    for review in reviews:
        counts = review[6:]
        _review.append({
            'text': review[5],
            'review_id': review[2],
            'stars': review[4],
            'num_keywords': list(zip(map(itemgetter(0), map(itemgetter(0), keywords)), counts)),
        })


    START = timeit.default_timer()
    sql, binds = getIndexSQL(keywords, review_ids, city)
    raw = sql % tuple(binds)
    #print raw
    index = list(engine.execute(Text(raw)))
    time = timeit.default_timer() - START
    print 'INDEX TIME:', time

    # this is absolutely unreadable
    index = {review_id: map(itemgetter(2,3,1), idx) for review_id, idx in groupby(index, key=itemgetter(0))}

    return json.dumps({'index': index, 'review': _review})

@app.route("/reviews/<business_id>/<city>/")
@cross_origin(origin='*')
def api_reviews_business(business_id, city):

    START = timeit.default_timer()
    sql, binds = getBusinessReviewSQL(business_id, city, limit=30)
    raw = sql % tuple(binds)
    print raw
    reviews = engine.execute(Text(raw))
    #[u'row', u'business_id', u'review_id', u'score', u'stars', u'text', u'relax', u'thai', u'noodl']
    reviews = list(reviews)
    review_ids = map(itemgetter(2), reviews)
    time = timeit.default_timer() - START
    print 'Review LIST TIME:', time

    _review = []
    for review in reviews:
        _review.append({
            'text': review[2],
            'review_id': review[0],
            'stars': review[1],
            'num_keywords': []
        })


    return json.dumps({'index': {}, 'review': _review})


