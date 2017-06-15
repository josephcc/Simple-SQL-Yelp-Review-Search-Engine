# coding: utf-8
import pdb

from operator import *

from gensim.models.doc2vec import Doc2Vec

from modelYelp import *

model = None
def loadModel(fn):
    global model
    model = Doc2Vec.load(fn)
    return model

from gensim import matutils
from numpy import dot
from scipy.spatial.distance import cosine

def search(keywords, topn=10):
    print keywords
    print
    weights = map(itemgetter(1), keywords)
    keywords = map(itemgetter(0), keywords)

    for keyword in keywords:
        if ':' in keyword:
            print keyword+':', '\t', model.most_similar([model.docvecs[keyword]])
        else:
            print keyword+':', '\t', model.most_similar(keyword)
    print

    def getVec(keyword):
        if ':' in keyword:
            return model.docvecs[keyword]
        return model[keyword]

    print keywords
    vecs = map(getVec, keywords)
    vecs = [vec*weight for vec, weight in zip(vecs, weights)]
    meanVec = sum(vecs)/sum(weights)

    results = model.docvecs.most_similar([meanVec], topn=topn*100*5)
    results = filter(lambda r: r[0].startswith('REST:'), results)

    n = 0
    for rank, result in enumerate(results):
        bid = result[0][5:]
        business = Business.get(business_id=bid)
        if business.city != 'Pittsburgh':
            n += 1
            continue
        ppvec = model.docvecs[result[0]]
        print '[%d]' % (rank - n), result[1]
        print business.name, '@', business.stars
        #print model.similar_by_vector( model.docvecs[result[0]])
        scores = []
        for vec in vecs:
            scores.append(dot(matutils.unitvec(ppvec), matutils.unitvec(vec)))
        nscores = [100*s/sum(scores) for s in scores]
        for kw, nscore, score in zip(keywords, nscores, scores):
            print ('%.1f%%' % nscore).zfill(5), '%.3f' % score, kw

        reviews = list(business.reviews)
        def _score(review):
            return cosine(matutils.unitvec(meanVec), matutils.unitvec(model.docvecs['REV:%s' % review.review_id]))
        reviews.sort(key=lambda r: _score(r))
        for review in reviews[:2] + reviews[-2:]:
            print _score(review)
            print review.text.encode('utf8')
            print '-' * 55

        print


loadModel('yelp.model')

#search([('innovation', 2), ('creativity', 1), ('brainstorming', 1), ('crowdsourcing', 1)], topn=50)
#search([('innovation', 2), ('creativity', 1), ('crowdsourcing', 1)], topn=100)
#search([('accessibility', 2), ('crowdsourcing', 1)], topn=100)
#search([('accessibility', 1), ('blind', 2), ('crowdsourcing', 1)], topn=100)

import sys
if __name__ == '__main__':
    stuff = sys.argv[1:]
    assert(len(stuff) % 2 ==0)
    keywords = stuff[::2]
    weights = map(float, stuff[1::2])

    search(list(zip(keywords, weights)), 100)

