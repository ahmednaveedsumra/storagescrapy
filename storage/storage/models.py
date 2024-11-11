from sqlalchemy import create_engine, Column, Integer,Date, String, Float, JSON, DateTime, UniqueConstraint, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.sql import func

Base = declarative_base()

class Facility(Base):
    __tablename__ = 'facilities'

    id = Column(Integer, primary_key=True, autoincrement=True)
    location = Column(JSON, nullable=True)
    name = Column(String(100), nullable=False)
    url = Column(String(250), unique=True, nullable=False)
    rating = Column(String(10), nullable=True)
    phone = Column(String(20), nullable=True)
    address = Column(String(250), nullable=True)
    zipcode = Column(String(20), nullable=True)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)

    units = relationship("Unit", back_populates="facility")

class Unit(Base):
    __tablename__ = 'units'

    id = Column(Integer, primary_key=True, autoincrement=True)
    facility_id = Column(Integer, ForeignKey('facilities.id'), nullable=False)  # Foreign key to Facility
    unit_name = Column(String(100), nullable=False)
    storage_type = Column(String(100), nullable=True)
    current_price = Column(Float, nullable=True)
    old_price = Column(Float, nullable=True)
    features = Column(String(250), nullable=True)
    availability = Column(String(100), nullable=True)
    promotion = Column(String(100), nullable=True)
    unit_url = Column(String(250), nullable=False)
    created_at_date = Column(Date, default=func.current_date(), nullable=False)
    __table_args__ = (
        UniqueConstraint('facility_id', 'created_at_date', 'storage_type', 'unit_url', name='unique_unit_per_day'),
    )

    facility = relationship("Facility", back_populates="units")

CSV_FILE_PATH = 'output.csv'
TABLE_NAME = "csvdata"

engine = create_engine('mysql+mysqlconnector://root:ahmad09102@localhost:3306/storage')
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

