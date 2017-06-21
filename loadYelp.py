import sys
import json

from modelYelp import *

from sqlalchemy.orm import scoped_session, sessionmaker

DBSession = scoped_session(sessionmaker())
DBSession.configure(bind=engine, autoflush=False, expire_on_commit=False)


basePath = './corpus/YELP'
businessFilePath = basePath + '/yelp_academic_dataset_business.json'
reviewFilePath = basePath + '/yelp_academic_dataset_review.json'


print 'loading business'
n = 0
_n = 0
bids = set([])
for business in open(businessFilePath):
    _n += 1
    if _n % 1000 == 0:
        print '%.2f%%' % (100.0 * _n / 144072), 
        sys.stdout.flush()
    business = json.loads(business)
    if business['categories'] == None or 'Restaurants' not in business['categories']:
        continue

    businessObj = {k:business[k] for k in ['business_id', 'stars', 'name', 'review_count', 'state', 'city']}

    businessObj = Business(**businessObj)
    DBSession.add(businessObj)

    bids.add(business['business_id'])
    for category in business['categories']:
        if category == 'Restaurants':
           continue
        categoryObj = Category(name=category, business_id=business['business_id'])
	DBSession.add(categoryObj)
    
    n += 1
    if n % 10000 == 0:
        print 'flush',
        sys.stdout.flush()
	DBSession.flush()

DBSession.commit()
print
print n
        


# In[4]:

n = 0
_n = 0
for review in open(reviewFilePath):
    _n += 1
    if _n % 10000 == 0:
        print '%.2f%%' % (_n / 41531.500), 
        sys.stdout.flush()
    review = json.loads(review)
    if not review['business_id'] in bids:
        continue
    businessObj = DBSession.query(Business).filter(Business.business_id ==  review['business_id']).one()
    attrs = {k:review[k] for k in ['review_id', 'stars', 'text']}
    attrs['business_id'] = businessObj.business_id
    attrs['bstars'] = businessObj.stars
    attrs['city'] = businessObj.city
    reviewObj = Review(**attrs)
    DBSession.add(reviewObj)
    
    n += 1
    if n % 10000 == 0:
        print 'flush',
        sys.stdout.flush()
	DBSession.flush()

DBSession.commit()
print 'done'
print n

