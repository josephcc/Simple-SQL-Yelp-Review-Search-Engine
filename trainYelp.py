import logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

from gensim.models.doc2vec import Doc2Vec, LabeledSentence

from nltk.tokenize import wordpunct_tokenize as TOK
from nltk.corpus import stopwords
STOPWORDS = set(stopwords.words('english'))

from modelYelp import *

import re
_digits = re.compile('\w+\d+')
_symbols = re.compile('\W')

def clean(tokens):
    tokens = map(lambda tok: tok.lower(), tokens)
    tokens = filter(lambda tok: tok not in STOPWORDS, tokens)
    tokens = [tok.strip() for tok in tokens]
    tokens = filter(lambda tok: len(tok) >= 3, tokens)
    tokens = filter(lambda tok: len(tok) <= 20, tokens)
    tokens = filter(lambda tok: tok.isdigit() == False, tokens)
    tokens = filter(lambda tok: not(_digits.match(tok)), tokens)
    tokens = filter(lambda tok: not(_symbols.search(tok)), tokens)

    return tokens

class YelpIterator(object):
    def __iter__(self):
        for business in Business.select():
            categories = ['CAT:%s' % cat.name for cat in business.categories]
            for review in business.reviews:
                #labels = categories + ['REST:%s' % business.business_id, 'REV:%s' % review.review_id, 'CITY:%s' %  business.city] 
                labels = categories
                tokens = TOK(review.text) + TOK(business.name)
                tokens = clean(tokens)
                yield LabeledSentence(words=tokens, tags=labels)



if __name__ == '__main__':
    model = Doc2Vec(YelpIterator(), size=300, workers=8, min_count=10)
    model.save('yelp_simple.model')

