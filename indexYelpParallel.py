import timeit
import logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

import Queue
from multiprocessing import JoinableQueue, Process, Value, TimeoutError, Lock

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
LIMIT = 165
TOTAL = Review.select().count()
COUNT = 0
STOP = Value('b', False)

def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]

START = timeit.default_timer()
def writer(q):
    global COUNT
    #while STOP.value != True or (not q.empty()):
    while (not q.empty()):
        try:
            cache = q.get(True, 2)
        except TimeoutError:
            continue
        except Queue.Empty:
            continue
#        for _cache in chunks(cache, LIMIT):
#            print _cache
#            Index.insert_many(_cache).execute()

        for row in cache:
            Index.create(**row)

        COUNT += 1
        time = timeit.default_timer() - START
        eta = time / (float(COUNT)/TOTAL)
        logging.info('%.2f%% ETA: %.2f minutes' % ((100.0*COUNT/TOTAL), eta/60))
        q.task_done()
    print 'writer out', STOP.value

Q1 = JoinableQueue(1280)
Q2 = JoinableQueue(10000)

def index(bid, rid, city, tokens, isName):
    pass

def worker(in_q, out_q):
    while STOP.value != True or (not in_q.empty()):
        try:
            review = in_q.get(True, 2)
        except TimeoutError:
            continue
        except Queue.Empty:
            continue


        tokens = TOK(review['review'])
        tokens = clean(tokens)
        cache = []
        for idx, token in tokens:
            cache.append({
                'token':	token,
                'index':	idx,
                'business_id':	review['business_id'],
                'review_id':	review['review_id'],
                'city':	review['city'],
                'isName':	review['isName']
            })
        out_q.put(cache)
        in_q.task_done()

# parallelize this
P1s = []
for i in range(8):
    P1 = Process(target=worker, args=(Q1,Q2))
    P1.start()
    P1s.append(P1)


i = 0
for business in Business.select():
    i += 1

    bid = business.business_id
    city = business.city
    review = {
        'review': business.name,
        'business_id':  bid,
        'review_id':  '',
        'city':  city,
        'isName':  True
    }
    if Q2.full():
        writer(Q2)

    try:
        Q1.put(review, True, 1)
    except:
        writer(Q2)
        Q1.put(review)
        

    for review in business.reviews:
        rid = review.review_id

        review = {
            'review': review.text,
            'business_id':  bid,
            'review_id':  rid,
            'city':  city,
            'isName':  False
        }
        try:
            Q1.put(review, True, 1)
        except:
            writer(Q2)
            Q1.put(review)

Q1.join()
print 'join q1'

STOP.value = True
print 'setstop'
for p in P1s:
    if p.is_alive():
        print 'a'
        p.join(2)
        print 'b'
print 'join p1'

writer(Q2)
Q2.join()
print 'join q2'

db.close()

