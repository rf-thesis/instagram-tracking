# create table for the search 
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ElasticsearchException
import config

DeclarativeBase = declarative_base()
db_url = config.db_url
index_name = "instagram"
doc_type = "insta"


mapping = {
    "properties": {
        "coordinates": {
            "properties": {
                "location": {
                    "type": "geo_point",
                    "lat_lon": True
                },
                "text": {
                    "type": "text",
                    "index": "not_analyzed"
                }
            }
        }
    }
}


def create_index(es, index_name, mapping):
    print('creating index {}...'.format(index_name))
    # es.indices.create(index_name, body={'mappings': mapping})


def check_index():
    es = Elasticsearch(host="localhost",
                       port=9200,
                       request_timeout=45)
    if es.indices.exists(index_name):
        print('index {} already exists'.format(index_name))
        try:
            es.indices.put_mapping(doc_type, mapping, index_name)
        except ElasticsearchException as e:
            print('error putting mapping:\n' + str(e))
            print('deleting index {}...'.format(index_name))
            es.indices.delete(index_name)
            create_index(es, index_name, mapping)
    else:
        print('index {} does not exist'.format(index_name))
        create_index(es, index_name, mapping)


def load_es(insta):
    es = Elasticsearch(host=config.es_host,
                       port=config.es_port,
                       request_timeout=45)
    instaid = insta["media_id"]
    result = es.index(index=index_name, doc_type="insta",
                      id=instaid, body=insta,
                      request_timeout=30)
    return result


def db_connect():
    return create_engine(db_url)


def create_db_session(engine):
    Session = sessionmaker(bind=engine, autoflush=False)
    session = Session()
    return session


def create_tables(engine):
    DeclarativeBase.metadata.create_all(engine)


class Insta(DeclarativeBase):
    __tablename__ = "instagram"

    media_id = Column(String, primary_key=True)
    user_id = Column(BigInteger)
    username = Column(String, nullable=True)
    fullname = Column(String, nullable=True)
    predicted_gender = Column(String, nullable=True)
    created_time = Column(DateTime, nullable=True)
    like_count = Column(BigInteger, nullable=True)
    comment_count = Column(BigInteger, nullable=True)
    media_text = Column(String, nullable=True)
    hashtags = Column(ARRAY(String), nullable=True)
    photo_url = Column(String, nullable=True)
    location_name = Column(String, nullable=True)
    longitude = Column(Float, nullable=True)
    latitude = Column(Float, nullable=True)
    location_id = Column(BigInteger, nullable=True)
    country = Column(String, nullable=True)
