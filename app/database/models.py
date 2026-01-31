from sqlalchemy import Column, Integer, String, Text, DateTime, JSON, BigInteger
from sqlalchemy.sql import func
from app.database.connection import Base

class Dataset(Base):
    __tablename__ = "datasets"

    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String, nullable=False)
    table_name = Column(String, unique=True, nullable=False)
    columns_info = Column(JSON, nullable=False)  # Store column profiling info
    row_count = Column(BigInteger, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

class DatasetSession(Base):
    __tablename__ = "dataset_sessions"

    id = Column(Integer, primary_key=True, index=True)
    session_id = Column(String, unique=True, nullable=False)  # UUID for session tracking
    dataset_id = Column(Integer, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())