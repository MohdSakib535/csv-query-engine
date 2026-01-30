from pydantic import BaseModel
from typing import Dict, Optional, List

class ColumnInfo(BaseModel):
    name: str
    type: str
    semantic_type: str

class UploadResponse(BaseModel):
    columns: List[ColumnInfo]

class QueryRequest(BaseModel):
    question: str
    use_ai: Optional[bool] = True

    class Config:
        json_schema_extra = {
            "example": {
                "question": "Which services were affected in Mumbai last month?",
                "use_ai": True
            }
        }

class QueryResult(BaseModel):
    sql: str
    rows: List[Dict]

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
