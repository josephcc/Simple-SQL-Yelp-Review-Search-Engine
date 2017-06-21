from operator import *
import timeit
import logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

import Queue
from multiprocessing import JoinableQueue, Process, Value, TimeoutError, Lock

from trainYelp import clean

#from nltk.tokenize import wordpunct_tokenize as TOK
from tokenizer import TreebankSpanTokenizer as tok
TOK = tok()
from nltk.corpus import stopwords
from nltk.stem.porter import *
stemmer = PorterStemmer()

STOPWORDS = set(stopwords.words('english') + ["n't"])
STEMSTOPWORDS = set(map(stemmer.stem, stopwords.words('english')))

from modelYelp import *

from sqlalchemy.orm import scoped_session, sessionmaker


DBSession = scoped_session(sessionmaker())
DBSession.configure(bind=engine, autoflush=False, expire_on_commit=False)

import re
_digits = re.compile('\w+\d+')
_symbols = re.compile('^\W+$')

def clean(tokens):
    tokens = map(lambda tok: (tok[0].lower(), tok[1]), tokens)
    tokens = [(tok[0].strip(), tok[1]) for tok in tokens]
    tokens = [(tok[0].strip('.'), tok[1]) for tok in tokens]
    tokens = list(enumerate(tokens)) #(0, ('a', (1, 2)))
    tokens = [(tok[0], tok[1][1][0], tok[1][1][1], tok[1][0]) for tok in tokens]
    # (idx, s, e, tok)
    tokens = filter(lambda tok: tok[-1] not in STOPWORDS, tokens)
    tokens = filter(lambda tok: len(tok[-1]) >= 3, tokens)
    tokens = filter(lambda tok: len(tok[-1]) <= 20, tokens)
    tokens = filter(lambda tok: tok[-1][0] != "'", tokens)
    tokens = filter(lambda tok: tok[-1].isdigit() == False, tokens)
    tokens = filter(lambda tok: not(_digits.match(tok[-1])), tokens)
    tokens = filter(lambda tok: not(_symbols.match(tok[-1])), tokens)

    tokens = [(idx, s, e, stemmer.stem(tok)) for idx, s, e, tok in tokens]
    tokens = filter(lambda tok: tok[-1] not in STEMSTOPWORDS, tokens)

    return tokens

COUNT = Value('i', 0)
TOTAL = DBSession.query(Review).count()
STOP = Value('b', False)

def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]

START = timeit.default_timer()

Q1 = JoinableQueue(2048)

def worker(in_q):
    engine.dispose()
    DBSession = scoped_session(sessionmaker())
    DBSession.configure(bind=engine, autoflush=False, expire_on_commit=False)

    _cache = []

    while STOP.value != True or (not in_q.empty()):
        try:
            review = in_q.get(True, 2)
        except TimeoutError:
            logging.info('WORKER TIMEOUT')
            continue
        except Queue.Empty:
            logging.info('WORKER QEMPTY')
            continue

        tokens = TOK.tokenize(review['review'], True)
        tokens = clean(tokens)
        cache = []
        for idx, s, e, token in tokens:
            cache.append({
                'token':	token,
                'index':	idx,
                'business_id':	review['business_id'],
                'review_id':	review['review_id'],
                'city':	review['city'],
                'isName':	review['isName'],
                'start': s,
                'end': e
            })
        _cache += cache
        in_q.task_done()
        with COUNT.get_lock():
            COUNT.value += 1

        if len(_cache) > 5000:
            DBSession.bulk_insert_mappings(Index, _cache)
            DBSession.commit()
            time = timeit.default_timer() - START
            eta = time / (float(COUNT.value)/TOTAL)
            logging.info('%.2f%% ETA: %.2f minutes (QSize: %d cache: %d)' % ((100.0*COUNT.value/TOTAL), eta/60, in_q.qsize(), len(_cache)))
            _cache = []

    if len(_cache) > 0:
        DBSession.bulk_insert_mappings(Index, _cache)
        _cache = []

    logging.info('DB COMMIT')
#    if len(_cache) > 0:
#        DBSession.bulk_insert_mappings(Index, _cache)
    DBSession.commit()

P1s = []
for i in range(7):
    P1 = Process(target=worker, args=(Q1,))
    P1.start()
    P1s.append(P1)

i = 0
for review in DBSession.query(Review).yield_per(2000):
    i += 1

    bid = review.business_id
    rid = review.review_id
    city = review.city
        
    review = {
        'review': review.text,
        'business_id':  bid,
        'review_id':  rid,
        'city':  city,
        'isName':  False
    }
    Q1.put(review)
    
Q1.join()
print 'join q1'

STOP.value = True
print 'setstop'
for p in P1s:
    if p.is_alive():
        print 'a'
        p.join()
        print 'b'
print 'join p1'

