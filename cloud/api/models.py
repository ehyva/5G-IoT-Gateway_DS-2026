from sqlalchemy import Column, Integer, String, Float, DateTime
from datetime import datetime
from database import Base

class Measurement(Base):
    __tablename__ = "measurements"

    id = Column(Integer, primary_key=True)
    sensor_id = Column(String, nullable=False)
    location = Column(String, nullable=False)
    sensor_type = Column(String, nullable=False)
    timestamp = Column(DateTime, nullable=False)
    sensor_value = Column(Float, nullable=False)