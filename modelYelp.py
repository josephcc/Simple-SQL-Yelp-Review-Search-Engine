from peewee import *
from playhouse.sqlite_ext import SqliteExtDatabase

dbPath = './corpus/YELP/yelp.db'
db = SqliteExtDatabase(dbPath)

class BaseModel(Model):
    class Meta:
        database = db

class Business(BaseModel):
#[u'city', u'neighborhood', u'name', u'business_id', u'longitude', u'hours', 
# u'state', u'postal_code', u'categories', u'stars', u'address', u'latitude',
# u'review_count', u'attributes', u'type', u'is_open']
    business_id = FixedCharField(unique=True)
    stars = FloatField(index=True)
    review_count = IntegerField(index=True)
    name = CharField()
    state = FixedCharField(index=True)
    city = FixedCharField(index=True)
    
class Category(BaseModel):
    business = ForeignKeyField(Business, related_name='categories')
    name = FixedCharField(index=True)

class Review(BaseModel):
# [u'funny', u'user_id', u'review_id', u'text', u'business_id', u'stars',
# u'date', u'useful', u'type', u'cool']
    review_id = FixedCharField(unique=True)
    business = ForeignKeyField(Business, related_name='reviews')
    stars = FloatField(index=True)
    text = TextField()
    
db.connect()

