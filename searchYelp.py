# coding: utf-8

import re
import logging
from flask import Flask, request
from flask_cors import CORS, cross_origin
app = Flask(__name__)
app.logger.setLevel(logging.DEBUG)
CORS(app)
cors = CORS(app, resources={"/*": {"origins": "*:*"}})

import sys
import json
from json import encoder
encoder.FLOAT_REPR = lambda o: format(o, '.8f')
import string
from itertools import *
from operator import *
from collections import *

import timeit

from jinjasql import JinjaSql
JSQL = JinjaSql()

from modelYelp import *
from sqlalchemy import text as Text


START = timeit.default_timer()
#from indexYelpVectors import getWordMentionsAndVectors
from gensim.models.doc2vec import Doc2Vec
model = Doc2Vec.load('./models/yelp.model')
time = timeit.default_timer() - START
print 'MODEL LOAD TIME:', time

import nltk
from nltk.stem.porter import *
stemmer = PorterStemmer()
from nltk.corpus import stopwords
STOPWORDS = set(stopwords.words('english')) - set(['not'])


#avgDL = engine.execute(Text('select avg(review_count) from business;')).first()[0]
#avgDL = float(avgDL)
#avgDLReview = engine.execute(Text('with innerv AS (select review_id, max(index) from index group by review_id limit 30) SELECT avg(max) from innerv;')).first()[0]
#avgDLReview = float(avgDLReview)
avgDL = 53.1536351449
avgDLReview = 118.666666667
k = 1.2 # 1.2 - 2.0
b = 0.75

numberOfBusinesses = dict(list(engine.execute(Text(r'''SELECT city, COUNT(business_id) FROM business GROUP BY city HAVING COUNT(business_id) > 1000;'''))))
print numberOfBusinesses

SQL_RankBusiness = open('SQL/rankBusiness.sql').read()
SQL_RankReview = open('SQL/rankReviews.sql').read()
SQL_GetReview = open('SQL/getReviews.sql').read()
SQL_GetMention = open('SQL/getMentions.sql').read()
SQL_Index = open('SQL/index.sql').read()
SQL_TermFreq = open('./SQL/termFreq.sql').read()
SQL_Business = open('./SQL/business.sql').read()
SQL_Category = open('./SQL/category.sql').read()
SQL_KeywordRatings = open('./SQL/keywordRatings.sql').read()

def getMentionsSQL(city, keywords, limitPerKeyword=5, context=100):
    sql, binds = JSQL.prepare_query(SQL_GetMention, {
        'city': city,
        'keywords': keywords,
        'limitPerKeyword': limitPerKeyword,
        'context': context
    })
    return sql, binds

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

def getReviewSQL(business_ids, avgDL, city, keywords, limit=3, stars=2.5):
    sql, binds = JSQL.prepare_query(SQL_RankReview, {
        'k': k,
        'b': b,
        'avgDL': avgDL,
        'city': city,
        'keywords': keywords,
        'positives': filter(lambda keyword: keyword[1] > 0, keywords),
        'business_ids': business_ids,
        'limit': limit,
        'stars': stars
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



def search(keywords, city, stars, allKeywords, limit=10):
    print keywords, city

    START = timeit.default_timer()
    sql, binds = getRankSQL(city, keywords, stars, limit)
    raw = sql % tuple(binds)
    #print raw
    ranks = engine.execute(Text(raw))
    ranks = list(ranks)
    time = timeit.default_timer() - START
    print 'RANK LIST TIME:', time

    START = timeit.default_timer()
    business_ids = map(itemgetter(0), ranks)
    sql, binds = getReviewSQL(business_ids, avgDLReview, city, keywords, limit=3, stars=stars)
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

def insertLog(logs):
    obj = Log()
    obj.turkerId = logs.get('turkerId', '')
    obj.api = logs.get('api', '')
    obj.action = logs.get('action', '')
    obj.condition = logs.get('condition', '')
    obj.content = json.dumps(logs)
    session = getSession()
    session.add(obj)
    session.commit()

@app.route("/search/<city>/<stars>/<keywords>/<weights>", methods=['POST', 'GET'])
@cross_origin(origin='*:*')
def api_search(city, stars, keywords, weights):
    if city == 'Montreal':
        city = u'Montréal'
    logs = request.get_json(force=True)
    if logs != None and logs['turkerId'] != 'nologging':
        logs['api'] = 'search'
        insertLog(logs)
        print logs
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

    payload = search(keywords, city, stars, allKeywords, 30)
    #payload['keywords'] = rawKeywords
    return json.dumps(payload)

@app.route("/baseline_search/<city>/<stars>/<keywords>/<weights>", methods=['POST', 'GET'])
@cross_origin(origin='*:*')
def api_baseline_search(city, stars, keywords, weights):
    if city == 'Montreal':
        city = u'Montréal'

    logs = request.get_json(force=True)
    if logs != None and logs['turkerId'] != 'nologging':
        logs['api'] = 'baseline_search'
        insertLog(logs)
        print logs

    print keywords
    keywords = keywords.split('|')
    rawKeywords = keywords
    keywords = [map(stemmer.stem, map(string.strip, keyword.split(','))) for keyword in keywords]
    rawKeywords = [map(string.strip, keyword.split(',')) for keyword in rawKeywords]

    n = float(numberOfBusinesses[city])
    _keywords = map(itemgetter(0), keywords)
    sql, binds = JSQL.prepare_query(SQL_TermFreq, {'city': city, 'keywords': _keywords})
    raw = sql % tuple(binds)
    stats = engine.execute(Text(raw))
    stats = {item[0]: n/item[-1] for item in stats}
    weights = [stats.get(keyword[0], 0.0) for keyword in keywords]


    keywords = list(zip(keywords, weights))
    allKeywords = keywords[:]
    keywords = filter(lambda k: k[1] != 0, keywords)
    rawKeywords = list(zip(rawKeywords, weights))

    payload = search(keywords, city, stars, allKeywords, 30)
    #payload['keywords'] = rawKeywords
    return json.dumps(payload)

@app.route("/expand/<city>/<keywords>", methods=['POST', 'GET'])
@cross_origin(origin='*:*')
def api_expand(city, keywords):
    if city == 'Montreal':
        city = u'Montréal'
    keywords = map(string.strip, keywords.split(','))
    candidates = model.most_similar(keywords, topn=1000)
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

        business_proportion = float(business_count) / numberOfBusinesses[city]

        if review_count <= 50 or business_count <= 3 or business_proportion >= 0.4:
            continue

        out2.append({'keyword': term, 'score': score, 'count': count, 'review_count': review_count, 'business_count': business_count, 'business_proportion': business_proportion})

    out2.sort(key=lambda x: x['score'], reverse=True)
    out2 = out2[:5]
    stems = map(stemmer.stem, map(itemgetter('keyword'), out2))

    sql, binds = getMentionsSQL(city, stems, limitPerKeyword=3, context=30)
    raw = sql % tuple(binds)
    mentions = engine.execute(Text(raw))
    mentions = map(list, mentions)
    stem2mentions = defaultdict(list)
    for mention in mentions:
        token, bid, rid, texts = mention[0], mention[1], mention[2], mention[3:]
        texts = map(lambda s: re.sub(r'\s\s+', ' ', s), texts)
        texts = map(string.strip, texts)
        stem2mentions[token].append( {'review_id': rid, 'mention': texts, 'business_id': bid} )

    out3 = []
    for o in out2:
        out3.append({
            'keyword': o['keyword'],
            'count': o['count'],
            'score': o['score'],
            'business_count': o['business_count'],
            'business_proportion': o['business_proportion'],
            'mentions': stem2mentions[stemmer.stem(o['keyword'])]
        })

    print len(out3)

    return json.dumps(out3)


@app.route("/business/<city>/<keyword>", methods=['POST', 'GET'])
@cross_origin(origin='*:*')
def api_business(city, keyword):
    logs = request.get_json(force=True)
    if logs != None and logs['turkerId'] != 'nologging':
        logs['api'] = 'business'
        insertLog(logs)
        print logs
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

@app.route("/reviews/<business_id>/<city>/<keywords>/<weights>", methods=['POST', 'GET'])
@cross_origin(origin='*:*')
def api_reviews(business_id, city, keywords, weights):
    if city == 'Montreal':
        city = u'Montréal'
    logs = request.get_json(force=True)
    if logs != None and logs['turkerId'] != 'nologging':
        logs['api'] = 'reviews'
        insertLog(logs)
        print logs
    print 'keywords', keywords
    keywords = keywords.split('|')
    rawKeywords = keywords
    keywords = [map(stemmer.stem, map(string.strip, keyword.split(','))) for keyword in keywords]
    weights = map(float, weights.split('|'))
    keywords = list(zip(keywords, weights))


    START = timeit.default_timer()
    limit = 30
    if len(keywords) == 1:
        limit = 100
    sql, binds = getReviewSQL([business_id], avgDLReview, city, keywords, limit=limit)
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
    print 'INDEX1'
    print keywords, review_ids, city
    raw = sql % tuple(binds)
    #print raw
    index = list(engine.execute(Text(raw)))
    time = timeit.default_timer() - START
    print 'INDEX TIME:', time
    # this is absolutely unreadable
    index = {review_id: map(itemgetter(2,3,1), idx) for review_id, idx in groupby(index, key=itemgetter(0))}

    payload = {'index': index, 'review': _review}

    if len(keywords) == 1 and len(_review) >= 10:
        winsize = 5
        charwinsize = winsize * 13
        stems = []
        stem2words = {}
        stem2review_ids = defaultdict(list)
        print 'KWKWKW'
        print stemmer.stem(keywords[0][0][0])
        stopwords = STOPWORDS | set([stemmer.stem(keywords[0][0][0])])
        for review in _review:
            start, end, _ = index[review['review_id']][0]
            text = review['text'][start-charwinsize:start] + ' ' + review['text'][end:end+charwinsize]
            _tokens = nltk.word_tokenize(text)[1:-1]
            _tokens = map(string.lower, _tokens)
            _tokens = filter(lambda token: len(token) > 3 and (not token in stopwords), _tokens)
            _stems = map(stemmer.stem, _tokens)
            stem2words.update(dict(zip(_stems, _tokens)))
            for stem in _stems:
                stem2review_ids[stem].append(review['review_id'])
            stems += list(set(_stems))
        stems = Counter(stems)
        tops = [(token, float(count)/len(_review)) for token, count in stems.most_common()[:20]]
        tops = filter(lambda x: x[1] > 0.14, tops)[:9]
        if len(tops) > 0:
            stems = [([stem], 1.0) for stem in map(itemgetter(0), tops)]

            review_ids = reduce(add, [stem2review_ids[stem] for stem, perc in tops])
            review_ids = list(set(review_ids))

            print len(_review), tops
            tops = [(stem2words[stem], perc) for stem, perc in tops]
            print len(_review), tops

            START = timeit.default_timer()
            sql, binds = getIndexSQL(stems, review_ids, city)
            print 'INDEX2'
            print stems, review_ids, city
            raw = sql % tuple(binds)
            #print raw
            index2 = list(engine.execute(Text(raw)))
            time = timeit.default_timer() - START
            print 'INDEX TIME:', time
            # this is absolutely unreadable
            index2 = {review_id: map(itemgetter(2,3,1), idx) for review_id, idx in groupby(index2, key=itemgetter(0))}
            print index2
            payload['comentions'] = {'tops': tops, 'index': index2}

    return json.dumps(payload)


@app.route("/baseline_reviews/<business_id>/<city>/<keywords>/<weights>", methods=['POST', 'GET'])
@cross_origin(origin='*:*')
def api_baseline_reviews(business_id, city, keywords, weights):
    if city == 'Montreal':
        city = u'Montréal'
    logs = request.get_json(force=True)
    if logs != None and logs['turkerId'] != 'nologging':
        logs['api'] = 'baseline_reviews'
        insertLog(logs)
        print logs
    print keywords
    keywords = keywords.split('|')
    rawKeywords = keywords
    keywords = [map(stemmer.stem, map(string.strip, keyword.split(','))) for keyword in keywords]

    n = float(numberOfBusinesses[city])
    _keywords = map(itemgetter(0), keywords)
    sql, binds = JSQL.prepare_query(SQL_TermFreq, {'city': city, 'keywords': _keywords})
    raw = sql % tuple(binds)
    stats = engine.execute(Text(raw))
    stats = {item[0]: n/item[-1] for item in stats}
    weights = [stats.get(keyword[0], 0.0) for keyword in keywords]

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

@app.route("/reviews/<business_id>/<city>", methods=['POST', 'GET'])
@cross_origin(origin='*')
def api_reviews_business(business_id, city):
    if city == 'Montreal':
        city = u'Montréal'
    logs = request.get_json(force=True)
    if logs != None and logs['turkerId'] != 'nologging':
        logs['api'] = 'review_business'
        insertLog(logs)
        print logs

    START = timeit.default_timer()
    sql, binds = getBusinessReviewSQL(business_id, city, limit=100)
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

@app.route("/mentions/<keyword>/<city>", methods=['POST', 'GET'])
@cross_origin(origin='*')
def api_mentions(keyword, city):
    if city == 'Montreal':
        city = u'Montréal'
    return json.dumps(getWordMentionsAndVectors(stemmer.stem(keyword), city, limit=50))

