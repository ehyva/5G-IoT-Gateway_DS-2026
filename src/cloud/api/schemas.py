from pydantic import BaseModel
from datetime import datetime

class MeasurementCreate(BaseModel):
    sensor_id: str
    location: str
    sensor_type: str
    timestamp: datetime
    sensor_value: float

class MeasurementResponse(MeasurementCreate):
    id: int

    class Config:
        from_attributes = True