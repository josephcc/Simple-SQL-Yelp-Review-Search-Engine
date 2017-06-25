from sqlalchemy import *
from sqlalchemy.schema import Index
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Integer, ForeignKey, String, Column, Float, Text, Boolean

DeclarativeBase = declarative_base()

class Business(DeclarativeBase):
    __tablename__ = 'business'

    business_id = Column(String, primary_key=True)
    stars = Column(Float, index=True)
    review_count = Column(Integer, index=True)
    name = Column(String)
    state = Column(String, index=True)
    city = Column(String, index=True)

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


DATABASE = {
  'drivername': 'postgres',
  'host': 'localhost',
  'port': '5432',
  'username': 'josephcc',
  'password': 'josephcc',
  'database': 'YELP'
}


engine = create_engine(URL(**DATABASE))
DeclarativeBase.metadata.create_all(engine)

from sqlalchemy.orm import scoped_session, sessionmaker

session_factory = sessionmaker(bind=engine)

def getSession():
    Session = scoped_session(session_factory)
    return Session

# r = S.query(Index.review_id, func.count(Index.review_id)).group_by(Index.review_id).filter(Index.city == 'Pittsburgh').filter(or_(Index.token == 'thai', Index.token == 'noodl')).order_by(func.count(I
#     ...: ndex.review_id).desc()).all()
