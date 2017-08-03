DROP FUNCTION IF EXISTS test(text[]);
DROP FUNCTION IF EXISTS test(text[], text);
DROP FUNCTION IF EXISTS ngramIndex(text[], text);
SET client_min_messages=DEBUG2;

CREATE FUNCTION ngramIndex(_ngrams text[], city text)
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

yieldStats = lambda stats: {
  'id': None,
  'city': city,
  'isName': False,
  'token': stats['token'],
  'business_id': stats['doc'][0],
  'review_id': stats['doc'][1],
  'index': stats['index'],
  'start': stats['start'],
  'end': stats['end']
}

for ngram in ngrams:

  offset0, token0 = ngram.pop(0) # this is slower than pop end

  for doc, array in index[token0].items():
    for aidx in range(len(array['indexs'])):
      stats = {
        'token': ' '.join([token0] + map(operator.itemgetter(1), ngram)),
        'doc': doc,
        'index': array['indexs'][aidx],
        'currentIndex': array['indexs'][aidx],
        'start': array['starts'][aidx],
        'end': array['ends'][aidx]
      }

      if len(ngram) == 0: # success terminal cond
        yield yieldStats(stats)

      _ngram = ngram[:]
      while len(_ngram) > 0:
        offset, token = _ngram.pop(0)
        while token == '*':
          offset, token = _ngram.pop(0)

        if stats['doc'] in index[token] and (stats['currentIndex'] + offset) in index[token][stats['doc']]['indexs']:
          stats['currentIndex'] += offset
          aidx = index[token][stats['doc']]['indexs'].index(stats['currentIndex'])
          stats['end'] = index[token][stats['doc']]['ends'][aidx]
        else:
          break

        if len(_ngram) == 0: # success terminal cond
          yield yieldStats(stats)

$$ LANGUAGE plpythonu IMMUTABLE;

SELECT * from ngramIndex(Array['spici taco', 'chicken * soup', 'tradit * * * * broth'], 'Pittsburgh')

