# coding: utf-8

print 'start'

import re
import string
from operator import * 
from itertools import *

from modelYelp import *
session = getSession()

from playgroundYelp import *

print 'model loaded'

import numpy as np

from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem.porter import *
stemmer = PorterStemmer()
STOPWORDS = set(stopwords.words('english')) - set(['not'])
STOPSTEMS = set(map(stemmer.stem, stopwords.words('english')))

mapmap = lambda f1, f2, L: map(f1, map(f2, L))
formap = lambda f, L: [map(f, _L) for _L in L]
forfilter = lambda f, L: [filter(f, _L) for _L in L]

winsize = 10
charwinsize = winsize * 13
alpha = 1.0 - (0.1**((winsize - 1) ** -1))


def getMostFrequentWords(city, limit=1000, offset=0):
    return ['lemon', 'basil', 'cocktail']
    words = session.execute('''
        SELECT token, COUNT(token)
        FROM index WHERE city = :city AND "isName" = false
        GROUP BY token ORDER BY Count DESC LIMIT :limit OFFSET :offset
    ''', {'city': city, 'limit': limit, 'offset': offset})

    return map(itemgetter(0), words)

def bow2vec(tokens):
    total = 0.0
    vectors = []
    for offset, token in enumerate(tokens):
        if not token in model:
            continue
        offset += 1
        weight = (1-alpha) ** (offset - 1)
        total += weight
        vectors.append(model[token] * weight)
    vectors = np.asarray(vectors)
    if total == 0.0:
        return []
    return vectors.sum(0)/total

def snippets2vector(snippets):
    snippets = map(word_tokenize, snippets)
    for index in range(0, len(snippets), 2):
        snippets[index] = list(reversed(snippets[index]))
    snippets = [snippet[:-1] for snippet in snippets]
    snippets = formap(string.lower, snippets)
    snippets = forfilter(lambda token: len(token) >= 3 and (not token in STOPWORDS), snippets)
    snippets = filter(len, snippets)
    vectors = map(bow2vec, snippets)
    vectors = filter(len, vectors)
    vectors = np.asarray(vectors)
    return vectors.sum(0)/float(len(vectors))

def getWordVectorsForBusiness(city, word):
    index = session.execute('''
        SELECT business_id, review_id, start, "end" FROM index
        WHERE city = :city AND "isName" = false AND token = :word ORDER BY business_id
    ''', {'city': city, 'word': word})

    payload = []
    for business_id, bgroup in groupby(index, key=itemgetter(0)):
        mentions = []
        for review_id, rgroup in groupby(bgroup, itemgetter(1)):
            substrings = []
            for start, end in map(itemgetter(2, 3), rgroup):
                substrings.append('substring(text, %d, %d)' % (max(0, start - charwinsize), charwinsize))
                substrings.append('substring(text, %d, %d)' % (end+1, charwinsize+1))
            substring = ', '.join(substrings)
            mention = session.execute('''
                SELECT %s
                FROM review WHERE city = :city and review_id = :review_id
            ''' % substring, {'city': city, 'review_id': review_id})
            mention = reduce(add, mention)
            mentions += mention
        vector = snippets2vector(mentions)
        payload.append({
            'city': city,
            'token': word,
            'business_id': business_id,
            'vector': vector.tolist()
        })
    return payload

def getWordMentionsAndVectors(word, city, limit=10):
    index = session.execute('''
        SELECT business_id, review_id, start, "end" FROM index
        WHERE city = :city AND "isName" = false AND token = :word
        ORDER BY random() LIMIT :limit
    ''', {'city': city, 'word': word, 'limit': limit})

    payload = []

    for business_id, review_id, start, end in index:
        substrings = []
        substrings.append('substring(text, %d, %d)' % (max(0, start - charwinsize), charwinsize))
        substrings.append('substring(text, %d, %d)' % (end+1, charwinsize+1))
        substring = ', '.join(substrings)

        mention = session.execute('''
            SELECT %s
            FROM review WHERE city = :city and review_id = :review_id
        ''' % substring, {'city': city, 'review_id': review_id})
        mention = reduce(add, mention)
        vector = snippets2vector(mention)
        payload.append({
            'mention': '%s *%s* %s' % (mention[0], word, mention[1]),
            'business_id': business_id,
            'vector': vector.tolist()
        })

    return payload

if __name__ == '__main__':
    import sys
    import datetime

    city = sys.argv[1]
    limit = int(sys.argv[2])
    offset = int(sys.argv[3])
    print limit, offset
    start = datetime.datetime.now()
    for index, word in enumerate(getMostFrequentWords(city, limit, offset)):
        if word in STOPSTEMS:
            continue
        print datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), '[%d]' % (index + 1), '\t', word, offset, limit, city
        payload = getWordVectorsForBusiness(city, word)
        session.bulk_insert_mappings(BusinessVector, payload)
        session.commit()

        elapse = (datetime.datetime.now() - start).total_seconds()
        eta = (limit - index - 1) * (elapse / (index + 1))
        print '  ETA:', str(datetime.timedelta(seconds=eta))


