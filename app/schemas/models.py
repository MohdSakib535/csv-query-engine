from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel


class ColumnInfo(BaseModel):
    name: str
    type: str
    semantic_type: str
    original_name: Optional[str] = None
    database_name: Optional[str] = None
    data_type: Optional[str] = None
    null_ratio: Optional[float] = 0.0
    top_values: Optional[List[str]] = []
    distinct_count: Optional[int] = 0
    min: Optional[float] = None
    max: Optional[float] = None
    mean: Optional[float] = None
    min_date: Optional[datetime] = None
    max_date: Optional[datetime] = None


class DatasetSummary(BaseModel):
    id: int
    filename: str
    table_name: str
    row_count: int
    columns_count: int
    created_at: datetime


class DatasetDetail(BaseModel):
    id: int
    filename: str
    table_name: str
    row_count: int
    columns: List[ColumnInfo]
    created_at: datetime


class UploadResponse(BaseModel):
    dataset_id: int
    table_name: str
    filename: str
    row_count: int
    columns: List[ColumnInfo]


class QueryRequest(BaseModel):
    question: str
    use_ai: Optional[bool] = True
    dataset_id: Optional[int] = None

    class Config:
        json_schema_extra = {
            "example": {
                "question": "Which services were affected in Mumbai last month?",
                "use_ai": True,
                "dataset_id": 1
            }
        }


class QueryResult(BaseModel):
    answer: str
    assumptions: List[str] = []
    sql: str
    table_preview: Optional[dict] = None
    chart: Optional[dict] = None
    rows: List[dict] = []  # Kept for backward compatibility if needed, but table_preview is preferred

    class Config:
        json_schema_extra = {
            "example": {
                "answer": "There were 15 Internet incidents in Mumbai.",
                "assumptions": ["Assumed 'Mumbai' refers to City"],
                "sql": "SELECT \"Service\", COUNT(*) as count FROM df WHERE \"City\" = 'Mumbai' GROUP BY \"Service\"",
                "table_preview": {
                     "columns": ["Service", "count"],
                     "rows": [["Internet", 15], ["Phone", 8]]
                },
                "chart": {"type": "bar", "x": "Service", "y": "count"}
            }
        }
