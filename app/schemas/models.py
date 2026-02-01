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
    sql: str
    rows: List[dict]

    class Config:
        json_schema_extra = {
            "example": {
                "sql": "SELECT \"Service\", COUNT(*) as count FROM df WHERE \"City\" = 'Mumbai' GROUP BY \"Service\"",
                "rows": [
                    {"Service": "Internet", "count": 15},
                    {"Service": "Phone", "count": 8}
                ]
            }
        }
