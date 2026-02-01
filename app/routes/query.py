from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
import pandas as pd
import re
from datetime import date, datetime
from app.routes.upload import current_dataset, current_columns_info
from app.utils.sql_generator import generate_sql_rule_based, generate_sql_ai, validate_sql
from app.schemas.models import QueryRequest, QueryResult
from app.database.connection import get_db
from app.database.service import db_service
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter()

AVG_FUNCTION_PATTERN = re.compile(
    r'AVG\(\s*(?:"(?P<quoted>[^"]+)"|(?P<plain>[^\s,)]+))\s*\)',
    re.IGNORECASE
)


def _date_column_names(columns_info):
    return {
        col.get("name")
        for col in columns_info
        if col.get("name")
        and (
            col.get("type") == "date"
            or col.get("semantic_type") == "date"
        )
    }


def _rewrite_avg_on_date_columns(sql: str, columns_info):
    date_columns = _date_column_names(columns_info)
    if not date_columns:
        return sql

    def _replace(match):
        column_name = match.group("quoted") or match.group("plain")
        if column_name in date_columns:
            quoted = f'"{column_name}"'
            return f"TO_TIMESTAMP(AVG(EXTRACT(EPOCH FROM {quoted})))"
        return match.group(0)

    return AVG_FUNCTION_PATTERN.sub(_replace, sql)

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
async def run_query(
    request: QueryRequest,
    session: AsyncSession = Depends(get_db)
):
    logger.info("Query endpoint called")

    # If no current dataset in memory, try to get the most recent one from database
    global current_dataset, current_columns_info
    if current_dataset is None:
        # Get the most recent dataset
        result = await session.execute(
            text("SELECT * FROM datasets ORDER BY created_at DESC LIMIT 1")
        )
        dataset_row = result.fetchone()
        if dataset_row:
            # Convert to Dataset object
            from app.database.models import Dataset
            current_dataset = Dataset(
                id=dataset_row.id,
                filename=dataset_row.filename,
                table_name=dataset_row.table_name,
                columns_info=dataset_row.columns_info,
                row_count=dataset_row.row_count,
                created_at=dataset_row.created_at,
                updated_at=dataset_row.updated_at
            )
            # Use cleaned column names for SQL generation
            current_columns_info = dataset_row.columns_info
            logger.info(f"Loaded dataset from database: {current_dataset.table_name}")

    if current_dataset is None:
        logger.error("No CSV uploaded")
        raise HTTPException(status_code=400, detail="No CSV uploaded")

    logger.info(f"Received data: {request.dict()}")
    question = request.question
    use_ai = request.use_ai if request.use_ai is not None else True

    if not question:
        logger.error("Question required")
        raise HTTPException(status_code=400, detail="Question required")

    columns = [col['name'] for col in current_columns_info]
    logger.info(f"Columns: {columns}")

    if not _looks_like_dataset_query(question, current_columns_info):
        raise HTTPException(status_code=400, detail="Sorry, I didn't get your query for this CSV")

    try:
        if use_ai:
            sql = generate_sql_ai(question, current_columns_info)
        else:
            sql = generate_sql_rule_based(question, current_columns_info)
        logger.info(f"Generated SQL: {sql}")

        # Convert DuckDB SQL to PostgreSQL SQL
        sql = _convert_sql_to_postgres(sql, current_dataset.table_name)
        logger.info(f"Converted PostgreSQL SQL: {sql}")
        logger.info(f"Table name: {current_dataset.table_name}")

        sql = validate_sql(sql, columns)
        logger.info(f"Validated SQL: {sql}")

        sql = _rewrite_avg_on_date_columns(sql, current_columns_info)
        logger.info(f"Adjusted SQL for date averages: {sql}")

        # Execute query using PostgreSQL
        raw_rows = await db_service.execute_query(sql, session)
        logger.info(f"Query executed successfully, rows: {len(raw_rows)}")

        # Convert to DataFrame for processing
        if raw_rows:
            df = pd.DataFrame(raw_rows)
            df = df.where(pd.notna(df), None)

            # Process results to handle duplicates (same logic as before)
            if len(df) > 0:
                if df.duplicated().any():
                    # Group by all columns and count
                    grouped = df.groupby(list(df.columns)).size().reset_index(name='count')
                    # Sort by count descending
                    grouped = grouped.sort_values('count', ascending=False)
                    grouped = grouped.where(pd.notna(grouped), None)
                    rows = [
                        {key: _normalize_value(value) for key, value in row.items()}
                        for row in grouped.to_dict('records')
                    ]
                    logger.info(f"Grouped {len(df)} rows into {len(grouped)} unique combinations")
                else:
                    rows = [
                        {key: _normalize_value(value) for key, value in row.items()}
                        for row in df.to_dict('records')
                    ]
            else:
                rows = []
        else:
            rows = []

        rows = _add_percentage(rows, question)
        return QueryResult(sql=sql, rows=rows)

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Error executing query: {str(e)}")

def _convert_sql_to_postgres(sql: str, table_name: str) -> str:
    """Convert DuckDB SQL to PostgreSQL SQL"""
    # Replace table reference
    sql = sql.replace('df', table_name)

    # Handle strftime function (DuckDB) to to_char (PostgreSQL)
    sql = re.sub(r"strftime\('([^']+)', ([^)]+)\)", r"to_char(\2, '\1')", sql)

    # Handle date functions
    sql = sql.replace('DATE_TRUNC', 'date_trunc')

    # Handle ILIKE for case-insensitive search
    sql = sql.replace('ILIKE', 'ILIKE')

    return sql
