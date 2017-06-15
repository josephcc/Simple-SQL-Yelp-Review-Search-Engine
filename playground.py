# coding: utf-8
import pdb

from operator import *

from gensim.models.doc2vec import Doc2Vec

model = None
def loadModel(fn):
    global model
    model = Doc2Vec.load(fn)
    return model

#from count import *

#C = reduce(lambda x,y:x+y, counters.values())
def similar(keywords, negative=[], topn=30, mode='keywords'):
  if type(keywords) == str:
    keywords = [keywords]
  similar = model.most_similar(positive=keywords, negative=negative, topn=topn*3)

  for keyword in keywords:
    print '%s' % (keyword)
    print '%.3f  %s' % (v,k)

import cPickle as pickle
PP = pickle.load(open('./papers.pickle'))

from gensim import matutils
from numpy import dot

def search(keywords, topn=10):
    print keywords
    print
    weights = map(itemgetter(1), keywords)
    keywords = map(itemgetter(0), keywords)

    for keyword in keywords:
        print keyword+':', '\t', model.most_similar(keyword)
    print

    vecs = map(model.__getitem__, keywords)
    vecs = [vec*weight for vec, weight in zip(vecs, weights)]
    meanVec = sum(vecs)/sum(weights)

    results = model.docvecs.most_similar([meanVec], topn=topn*10)
    results = filter(lambda x: 'PP:' in x[0], results)[:topn]
    for rank, result in enumerate(results):
        pp = ':'.join(result[0].split(':')[1:])
        pp = PP[pp]
        ppvec = model.docvecs[result[0]]
        print '[%d]' % rank, result[1]
        #print result[0]
        #print model.similar_by_vector( model.docvecs[result[0]])
        scores = []
        for vec in vecs:
            scores.append(dot(matutils.unitvec(ppvec), matutils.unitvec(vec)))
        nscores = [100*s/sum(scores) for s in scores]
        for kw, nscore, score in zip(keywords, nscores, scores):
            print ('%.1f%%' % nscore).zfill(5), '%.3f' % score, kw
        print pp.title, '(', ':'.join(result[0].split(':')[1:]), ')'
        print '\t', pp.authors
        print '\t', pp.keywords
        print

loadModel('kw2vec_full.model')

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

