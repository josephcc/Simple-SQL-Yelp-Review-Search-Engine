import timeit
import logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

from trainYelp import clean

from nltk.tokenize import wordpunct_tokenize as TOK
from nltk.corpus import stopwords
from nltk.stem.porter import *
stemmer = PorterStemmer()

STOPWORDS = set(stopwords.words('english'))
STEMSTOPWORDS = set(map(stemmer.stem, stopwords.words('english')))

from modelYelp import *

import re
_digits = re.compile('\w+\d+')
_symbols = re.compile('^\W+$')

def clean(tokens):
    tokens = map(lambda tok: tok.lower(), tokens)
    tokens = [tok.strip() for tok in tokens]
    tokens = list(enumerate(tokens))
    tokens = filter(lambda tok: tok[1] not in STOPWORDS, tokens)
    tokens = filter(lambda tok: len(tok[1]) >= 3, tokens)
    tokens = filter(lambda tok: len(tok[1]) <= 20, tokens)
    tokens = filter(lambda tok: tok[1].isdigit() == False, tokens)
    tokens = filter(lambda tok: not(_digits.match(tok[1])), tokens)
    tokens = filter(lambda tok: not(_symbols.match(tok[1])), tokens)

    tokens = [(idx, stemmer.stem(tok)) for idx, tok in tokens]
    tokens = filter(lambda tok: tok[1] not in STEMSTOPWORDS, tokens)

    return tokens

CACHE = []
ROWS = 20000
LIMIT = 16
TOTAL = Review.select().count()
COUNT = 0
def index(bid, rid, city, tokens, isName):
    for idx, token in tokens:
        CACHE.append({
            'token':  token,
            'business_id':  bid,
            'review_id':  rid,
            'index':  idx,
            'city':  city,
            'isName':  isName
        })

def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]

START = timeit.default_timer()
def clear():
    global CACHE
    if len(CACHE) < ROWS:
        return
#    for cache in chunks(CACHE, LIMIT):
#        Index.insert_many(cache).execute()
    for row in CACHE:
        Index.create(**row)
    CACHE = []
    time = timeit.default_timer() - START
    eta = time / (float(COUNT)/TOTAL)
    logging.info('%.2f%% ETA: %.2f minutes' % ((100.0*COUNT/TOTAL), eta/60))


with db.atomic():
    # parallelize this
    for business in Business.select():
        bid = business.business_id
        city = business.city
        tokens = TOK(business.name)
        tokens = clean(tokens)
        index(bid, '', city, tokens, True)
        for review in business.reviews:
            COUNT += 1
            rid = review.review_id
            tokens = TOK(review.text)
            tokens = clean(tokens)
            index(bid, rid, city, tokens, False)
            clear()
    clear()

