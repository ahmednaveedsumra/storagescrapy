from sqlalchemy import create_engine, Column, Integer, String, Float, JSON, DateTime, UniqueConstraint, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.sql import func

Base = declarative_base()

class Facility(Base):
    __tablename__ = 'facilities'

    id = Column(Integer, primary_key=True, autoincrement=True)
    location = Column(JSON, nullable=True)
    name = Column(String(100), nullable=False)
    url = Column(String(250), unique=True, nullable=False)  # Unique constraint on URL
    rating = Column(String(10), nullable=True)
    phone = Column(String(20), nullable=True)
    address = Column(String(250), nullable=True)
    zipcode = Column(String(20), nullable=True)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)

    # One-to-many relationship with Unit
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
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)

    # Unique constraint on unit_name and unit_url
    __table_args__ = (
        UniqueConstraint('storage_type', 'unit_url', name='uix_storage_type_url'),
    )

    # Relationship to Facility
    facility = relationship("Facility", back_populates="units")

# Database setup
engine = create_engine('mysql+mysqlconnector://root:ahmad09102@localhost:3306/storage')
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
