import cPickle as pickle
from operator import *
from collections import defaultdict, Counter

from gensim.models.doc2vec import LabeledSentence

from utils import *

docs = pickle.load(open('documents_full.pickle'))

counters = defaultdict(Counter)
for doc in docs:
    _, conf, year, pid = filter(lambda s: s[:3] == 'PP:', doc.tags)[0].split(':')
    year = int(year)
    keywords = doc.tags[:-1]
    keywords = [keyword[4:] for keyword in keywords]

    counters[year].update(keywords)

keys = [map(itemgetter(0), counter.most_common(5)) for counter in counters.values()]
keys = set(reduce(lambda x,y:x+y, keys))

for key in keys:
    print key, '\t',
    for year in sorted(counters.keys()):
        print counters[year][key], '\t',
    print



