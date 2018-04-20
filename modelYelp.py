from sqlalchemy import *
from sqlalchemy.schema import Index
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Integer, ForeignKey, String, Column, Float, Text, Boolean
from geoalchemy2 import Geography


DeclarativeBase = declarative_base()

class Business(DeclarativeBase):
    __tablename__ = 'business'

    business_id = Column(String, primary_key=True)
    stars = Column(Float, index=True)
    review_count = Column(Integer, index=True)
    name = Column(String)
    address = Column(String)
    state = Column(String, index=True)
    city = Column(String, index=True)
    location = Column(Geography(geometry_type='POINT', srid=4326))

    categories = relationship("Category", back_populates="business")
    reviews = relationship("Review", back_populates="business")

    
class Category(DeclarativeBase):
    __tablename__ = 'category'

    id = Column(Integer, primary_key=True)
    name = Column(String, index=True)

    business_id = Column(String, ForeignKey('business.business_id'))
    business = relationship("Business", back_populates="categories")


class Review(DeclarativeBase):
    __tablename__ = 'review'

    review_id = Column(String, primary_key=True)
    bstars = Column(String, index=True)
    stars = Column(Float, index=True)
    text =  Column(Text)
    city = Column(String, index=True)

    business_id = Column(String, ForeignKey('business.business_id'))
    business = relationship("Business", back_populates="reviews")

class BusinessVector(DeclarativeBase):
    __tablename__ = 'business_token_vector_pittsburgh'

    id = Column(Integer, primary_key=True)
    vector = Column('vector', ARRAY(Float))
    business_id = Column(String, index=True)
    token = Column(String, index=True)
    city = Column(String, index=True)

    __table_args__ = (
        Index('idx_city_business_token_pittsburgh', 'city', 'business_id', 'token'),
    )

    
class Index(DeclarativeBase):
    __tablename__ = 'index'

    id = Column(Integer, primary_key=True)
    token = Column(String, index=True)
    business_id = Column(String, index=True)
    review_id = Column(String, index=True)
    index = Column(Integer, index=True)
    start = Column(Integer)
    end = Column(Integer)
    city = Column(String, index=True)
    isName = Column(Boolean, index=True)


    __table_args__ = (
        Index('idx_token_city_isName', 'token', 'city', 'isName'),
        Index('idx_business_review', 'business_id', 'review_id')
    )


class Log(DeclarativeBase):
    __tablename__ = 'log'

    id = Column(Integer, primary_key=True)
    turkerId = Column(String)
    api = Column(String)
    action = Column(String)
    condition = Column(String)
    content =  Column(Text)
    last_update = Column(TIMESTAMP, server_default=func.now(), onupdate=func.current_timestamp())


DATABASE = {
  'drivername': 'postgres',
  'host': 'localhost',
  'port': '5432',
  'username': 'josephcc',
  'password': 'josephcc',
  'database': 'yelp_new'
}

from sqlalchemy.pool import QueuePool

engine = create_engine(URL(**DATABASE), pool_size=30, max_overflow=10, poolclass=QueuePool)
DeclarativeBase.metadata.create_all(engine)

from sqlalchemy.orm import scoped_session, sessionmaker

session_factory = sessionmaker(bind=engine)

def getSession():
    Session = scoped_session(session_factory)
    return Session

# r = S.query(Index.review_id, func.count(Index.review_id)).group_by(Index.review_id).filter(Index.city == 'Pittsburgh').filter(or_(Index.token == 'thai', Index.token == 'noodl')).order_by(func.count(I
#     ...: ndex.review_id).desc()).all()

# select token, city, count(*) as term, count(distinct review_id) as review, count(distinct business_id) as business into docFreq from index group by token, city 
# create index idx_docFreq_token_city on docFreq (token, city)
