import cPickle as pickle

from gensim.models.doc2vec import Doc2Vec, LabeledSentence

from nltk.tokenize import wordpunct_tokenize as TOK
from nltk.corpus import stopwords
STOPWORDS = set(stopwords.words('english') + ['cid'])

from utils import *

PAPERS = {}

import re
_digits = re.compile('\w+\d+')
_symbols = re.compile('\W')

def clean(tokens):
    tokens = map(lambda tok: tok.lower(), tokens)
    tokens = map(lambda tok: tok.decode('utf8', 'ignore').encode('utf8', 'ignore'), tokens)
    tokens = filter(lambda tok: tok not in STOPWORDS, tokens)
    tokens = [tok.strip() for tok in tokens]
    tokens = filter(lambda tok: len(tok) >= 3, tokens)
    tokens = filter(lambda tok: len(tok) <= 20, tokens)
    tokens = filter(lambda tok: tok.isdigit() == False, tokens)
    tokens = filter(lambda tok: tok.endswith('chi') == False, tokens)
    tokens = filter(lambda tok: not(_digits.match(tok)), tokens)
    tokens = filter(lambda tok: not(_symbols.search(tok)), tokens)

    return tokens


documents = []
hci = 0
others = 0

for filename in ['wos.txt', 'pubmed.txt', 'microsoft.txt']:
    for line in open('papers/' + filename):
        tokens = TOK(line)
        tokens = clean(tokens)
        tokens = tokens * 20

        keywords = []
        '''
        keywords = map(lambda keyword: keyword.lower(), paper.keywords)
        keywords = map(lambda keyword: 'AKW:' + keyword, keywords)
        keywords.append('PP:CSCW:%s:%s' % (paper.year, paper.pid))
        for author in paper.authors:
            keywords.append('AUTH:%s' % author)
        '''
        sentence = LabeledSentence(words=tokens, tags=keywords)
        documents.append(sentence)
        others += len(tokens)

print len(documents)


for paper in iterSIGCHI():
    if len(paper.keywords) == 0:
        continue
    tokens = TOK(paper.text) + TOK(paper.title) + (TOK(paper.abstract) * int(round(float(len(paper.abstract))/len(paper.text)/2)))
    tokens = clean(tokens)

    keywords = []
    '''
    keywords = map(lambda keyword: keyword.lower(), paper.keywords)
    keywords = map(lambda keyword: 'AKW:' + keyword, keywords)
    keywords.append('PP:CHI:%s:%s' % (paper.year, paper.pid))
    for author in paper.authors:
        keywords.append('AUTH:%s' % author)
    '''
    sentence = LabeledSentence(words=tokens, tags=keywords)
    documents.append(sentence)

    #PAPERS['CHI:%s:%s' % (paper.year, paper.pid)] = paper
    hci += len(tokens)

for paper in iterCSCW():
    if len(paper.keywords) == 0:
        continue
    tokens = TOK(paper.text) + TOK(paper.title) + (TOK(paper.abstract) * int(round(float(len(paper.abstract))/len(paper.text)/2)))
    tokens = clean(tokens)

    keywords = []
    '''
    keywords = map(lambda keyword: keyword.lower(), paper.keywords)
    keywords = map(lambda keyword: 'AKW:' + keyword, keywords)
    keywords.append('PP:CSCW:%s:%s' % (paper.year, paper.pid))
    for author in paper.authors:
        keywords.append('AUTH:%s' % author)
    '''
    sentence = LabeledSentence(words=tokens, tags=keywords)
    documents.append(sentence)

    #PAPERS['CSCW:%s:%s' % (paper.year, paper.pid)] = paper
    hci += len(tokens)

print len(documents)
print 'HCI:', hci
print 'Others:', others

'''
fp = open('papers.pickle', 'w')
pickle.dump(PAPERS, fp, pickle.HIGHEST_PROTOCOL)
fp.close()
'''

model = Doc2Vec(documents, size=600, workers=8, min_count=20)
model.save('papers.model')

print model.most_similar(positive=['crowdsourcing'])

