from sqlalchemy import create_engine, Column, Integer, String, Float, JSON,DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

Base = declarative_base()

class Facility(Base):
    __tablename__ = 'facilities'

    id = Column(Integer, primary_key=True, autoincrement=True)
    location = Column(JSON)
    name = Column(String(100))
    url = Column(String(250))
    rating = Column(String(100))
    phone = Column(String(100))
    address = Column(String(250))
    zipcode = Column(String(100))
    unit_name = Column(String(100))
    storage_type = Column(String(100))
    current_price = Column(Float)
    old_price = Column(Float)
    features = Column(String(250))
    availability = Column(String(100))
    promotion = Column(String(100))
    unit_url = Column(String(250))
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

engine = create_engine('mysql+mysqlconnector://root:ahmad09102@localhost:3306/exceldata')


Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
