DROP FUNCTION IF EXISTS test(text[]);
DROP FUNCTION IF EXISTS test(text[], text);
SET client_min_messages=DEBUG2;

CREATE FUNCTION test(_ngrams text[], city text)
  RETURNS SETOF index
AS $$

import string
import operator
import itertools
from collections import defaultdict

ngrams = map(string.split, _ngrams)

tokens = set(itertools.chain.from_iterable(ngrams))
if '*' in tokens:
  tokens.remove('*')
tokens = list(tokens)

ngrams = map(lambda ngram: list(enumerate(ngram)), ngrams)

rv = plpy.execute(r'''
SELECT
    token,
    business_id,
    review_id,
    ARRAY_AGG(index) AS indexs,
    ARRAY_AGG(start) AS starts,
    ARRAY_AGG("end") AS ends
FROM
    index
WHERE
    city = '%s' AND
    "isName" = false AND
    token IN ('%s')
GROUP BY
    business_id, review_id, token
''' % (city, "', '".join(tokens)))

index = defaultdict(lambda: defaultdict(dict))
for r in rv:
  index[r['token']][(r['business_id'], r['review_id'])] = {'indexs': r['indexs'], 'starts': r['starts'], 'ends': r['ends']}

out = []

def progress(ngram, stats=None):

  if len(ngram) == 0: # success terminal cond
    out.append(stats)
    return

  offset, token = ngram.pop(0) # this is slower than pop end
  while token == '*':
    offset, token = ngram.pop(0) # this is slower than pop end
  
  if stats == None: # init cond
    for doc, array in index[token].items():
      for aidx in range(len(array['indexs'])):
        newStats = {
          'token': ' '.join([token] + map(operator.itemgetter(1), ngram)),
          'doc': doc,
          'index': array['indexs'][aidx],
          'currentIndex': array['indexs'][aidx],
          'start': array['starts'][aidx],
          'end': array['ends'][aidx]
        }
        progress(ngram[:], newStats)

  elif stats['doc'] in index[token] and (stats['currentIndex'] + offset) in index[token][stats['doc']]['indexs']:
    stats['currentIndex'] += offset
    aidx = index[token][stats['doc']]['indexs'].index(stats['currentIndex'])
    stats['end'] = index[token][stats['doc']]['ends'][aidx]
    progress(ngram, stats)

  # else: # fail terminal cond


for ngram in ngrams:
  progress(ngram[:])


for r in out:
  yield {
    'id': None,
    'city': city,
    'isName': False,
    'token': r['token'],
    'business_id': r['doc'][0],
    'review_id': r['doc'][1],
    'index': r['index'],
    'start': r['start'],
    'end': r['end']
  }


$$ LANGUAGE plpythonu;

SELECT * from test(Array['tradit * * * * broth', 'spici taco', 'spici', 'taco', 'stand', 'stood'], 'Pittsburgh')
