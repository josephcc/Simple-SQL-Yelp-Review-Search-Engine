# coding: utf-8

print 'start'

import re
import sys
import csv
import time
import random
import string
from textwrap import wrap
from operator import * 
from itertools import *
sys.path.append('../')

from modelYelp import *
session = getSession()

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
    return ['cocktail']
    return ['basil', 'lemon', 'cocktail']
    words = session.execute('''
        SELECT token, COUNT(token)
        FROM index WHERE city = :city AND "isName" = false
        GROUP BY token ORDER BY Count DESC LIMIT :limit OFFSET :offset
    ''', {'city': city, 'limit': limit, 'offset': offset})

    return map(itemgetter(0), words)

def getWordVectorsForBusiness(city, word):
    out = []
    index = session.execute('''
        SELECT business_id, review_id, start, "end" FROM index
        WHERE city = :city AND "isName" = false AND token = :word ORDER BY business_id
    ''', {'city': city, 'word': word})


    for business_id, bgroup in groupby(index, key=itemgetter(0)):
        print business_id
        bgroup = list(bgroup)
        review_ids = map(itemgetter(1), bgroup)
        review_ids = random.sample(review_ids, min(len(review_ids), 5))
        review_ids = set(review_ids)
        for review_id, rgroup in groupby(bgroup, itemgetter(1)):
            if not review_id in review_ids:
                continue
            substrings = []
            for start, end in map(itemgetter(2, 3), rgroup):
                substrings.append('substring(text, %d, %d)' % (max(0, start - charwinsize), charwinsize + (end - start) + charwinsize + 1))
            substring = ', '.join(substrings)
            mention = session.execute('''
                SELECT %s
                FROM review WHERE city = :city and review_id = :review_id
            ''' % substring, {'city': city, 'review_id': review_id})
            mention = list(mention)[0]
            mention = random.sample(mention, 1)[0]
            mention = mention.replace('\n', ' ')
            s1 = charwinsize+1 + min(0, start-charwinsize-1)
            s2 = s1 + end - start
            out.append({
                'pre': mention[:s1].encode('utf8'),
                'term': mention[s1:s2].encode('utf8'),
                'post': mention[s2:].encode('utf8'),
                'business_id': business_id,
            })

    return out

if __name__ == '__main__':
    import sys
    import datetime

    city = 'Pittsburgh'

    for index, word in enumerate(getMostFrequentWords(city)):
        if word in STOPSTEMS:
            continue

        out = getWordVectorsForBusiness(city, word)
        with open('%s.csv' % word, 'w') as csvfile:
            fieldnames = out[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for row in out:
                writer.writerow(row)

