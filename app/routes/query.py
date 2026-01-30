from fastapi import APIRouter, HTTPException
import duckdb
import pandas as pd
import re
from datetime import date, datetime
from app.routes import upload
from app.utils.sql_generator import generate_sql_rule_based, generate_sql_ai, validate_sql
from app.schemas.models import QueryRequest, QueryResult
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter()

def _normalize_value(value):
    if value is None:
        return None
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return value

def _looks_like_dataset_query(question: str, columns_info) -> bool:
    lower = (question or "").lower()
    if not lower:
        return False

    column_names = [col.get("name", "").lower() for col in columns_info]
    if any(name and name in lower for name in column_names):
        return True

    tokens = set()
    for name in column_names:
        for token in re.split(r"[_\\s]+", name):
            if len(token) >= 3:
                tokens.add(token)
    if any(token in lower for token in tokens):
        return True

    query_keywords = [
        "count", "average", "avg", "sum", "total", "trend", "top", "distribution",
        "percentage", "ratio", "group", "by", "compare", "min", "max", "median",
        "mean", "list", "show", "how many", "how much", "number of"
    ]
    return any(keyword in lower for keyword in query_keywords)

def _add_percentage(rows, question: str):
    if not rows:
        return rows
    lower_question = (question or '').lower()
    if not any(word in lower_question for word in ['percent', 'percentage', 'ratio', 'distribution']):
        return rows
    if any('percentage' in key.lower() for key in rows[0].keys()):
        return rows

    count_key = None
    for key in rows[0].keys():
        if key.lower() in ('count', 'cnt', 'total'):
            count_key = key
            break

    if not count_key:
        numeric_keys = [
            key for key, value in rows[0].items()
            if isinstance(value, (int, float)) and not isinstance(value, bool)
        ]
        if len(numeric_keys) == 1:
            count_key = numeric_keys[0]

    if not count_key:
        return rows

    total = sum(float(row.get(count_key) or 0) for row in rows)
    if total <= 0:
        return rows

    for row in rows:
        row['percentage'] = round((float(row.get(count_key) or 0) / total) * 100, 2)
    return rows

@router.post("/query", response_model=QueryResult)
async def run_query(request: QueryRequest):
    logger.info("Query endpoint called")
    if upload.uploaded_df is None:
        logger.error("No CSV uploaded")
        raise HTTPException(status_code=400, detail="No CSV uploaded")

    logger.info(f"Received data: {request.dict()}")
    question = request.question
    use_ai = request.use_ai if request.use_ai is not None else True

    if not question:
        logger.error("Question required")
        raise HTTPException(status_code=400, detail="Question required")

    columns = [col['name'] for col in upload.columns_info]
    logger.info(f"Columns: {columns}")

    if not _looks_like_dataset_query(question, upload.columns_info):
        raise HTTPException(status_code=400, detail="Sorry, I didn't get your query for this CSV")

    try:

        if use_ai:
            sql = generate_sql_ai(question, upload.columns_info)
        else:
            sql = generate_sql_rule_based(question, upload.columns_info)
        logger.info(f"Generated SQL: {sql}")

        sql = validate_sql(sql, columns)
        logger.info(f"Validated SQL: {sql}")

        con = duckdb.connect()
        con.register('df', upload.uploaded_df)
        result = con.execute(sql).fetchdf()
        con.close()
        logger.info(f"Query executed successfully, rows: {len(result)}")

        result = result.where(pd.notna(result), None)

        # Process results to handle duplicates
        if len(result) > 0:
            # Check if there are duplicate rows
            if result.duplicated().any():
                # Group by all columns and count
                grouped = result.groupby(list(result.columns)).size().reset_index(name='count')
                # Sort by count descending
                grouped = grouped.sort_values('count', ascending=False)
                grouped = grouped.where(pd.notna(grouped), None)
                rows = [
                    {key: _normalize_value(value) for key, value in row.items()}
                    for row in grouped.to_dict('records')
                ]
                logger.info(f"Grouped {len(result)} rows into {len(grouped)} unique combinations")
            else:
                rows = [
                    {key: _normalize_value(value) for key, value in row.items()}
                    for row in result.to_dict('records')
                ]
        else:
            rows = []

        rows = _add_percentage(rows, question)
        return QueryResult(sql=sql, rows=rows)
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Error executing query: {str(e)}")
