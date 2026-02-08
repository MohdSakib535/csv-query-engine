from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
import pandas as pd
import re
from datetime import date, datetime
from typing import List, Dict
from app.database.models import Dataset
from app.routes.upload import current_dataset, current_columns_info
from app.utils.sql_generator_v2 import generate_sql, validate_sql_safety
from app.utils.sql_generator import generate_sql_ai, validate_sql
from app.utils.query_planner_v2 import plan_query, QueryPlan
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

SQL_RESERVED_KEYWORDS = {
    "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "BETWEEN", "INNER", "LEFT", "RIGHT",
    "JOIN", "ON", "GROUP", "BY", "ORDER", "HAVING", "LIMIT", "OFFSET", "ASC", "DESC", "AS",
    "COUNT", "AVG", "SUM", "MAX", "MIN", "TO_TIMESTAMP", "EXTRACT", "EPOCH", "FUNCTION",
    "CAST", "COALESCE", "DATE_TRUNC", "ILIKE", "LIKE", "DISTINCT", "CASE", "WHEN", "THEN",
    "ELSE", "END", "TRUE", "FALSE", "NULL", "DATE", "TIMESTAMP", "NOW", "CURRENT_DATE",
    "INTERVAL", "OVER", "ROWS", "FETCH", "UNION", "ALL", "TOP", "PERCENT", "FILTER", "WITH",
    "VALUES", "TABLE", "DF", "MONTH", "YEAR"
}
IDENTIFIER_PATTERN = re.compile(r'"([^"]+)"|\b([A-Za-z_][A-Za-z0-9_]*)\b')
ALIAS_PATTERN = re.compile(r'\bAS\s+([A-Za-z_][A-Za-z0-9_]*)', re.IGNORECASE)


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


def _friendly_column_hint(columns_info: List[Dict], limit: int = 6) -> str:
    hints = []
    for col in columns_info:
        display = col.get("original_name") or col.get("name")
        if display:
            hints.append(display)
        if len(hints) >= limit:
            break
    return ", ".join(hints) if hints else "no columns available"


def _validate_sql_schema(sql: str, columns_info: List[Dict], table_name: str):
    allowed = {col.get("name", "") for col in columns_info if col.get("name")}
    allowed_lower = {name.lower() for name in allowed}

    tokens = {
        match[0] or match[1]
        for match in IDENTIFIER_PATTERN.findall(sql)
        if match[0] or match[1]
    }
    alias_tokens = {alias.upper() for alias in ALIAS_PATTERN.findall(sql)}

    invalid_columns = []
    for token in tokens:
        if not token:
            continue
        token_upper = token.upper()
        if token_upper in SQL_RESERVED_KEYWORDS:
            continue
        if token_upper in alias_tokens:
            continue
        if token.lower() == table_name.lower():
            continue
        if token.lower() in allowed_lower:
            continue
        if token.isdigit() or re.match(r'^\d+(\.\d+)?$', token):
            continue
        invalid_columns.append(token)

    if invalid_columns:
        friendly = _friendly_column_hint(columns_info)
        invalid_list = ", ".join(sorted(set(invalid_columns)))
        raise HTTPException(
            status_code=400,
            detail=(
                f"The query references unknown columns ({invalid_list}). "
                f"Available columns include: {friendly}."
            )
        )


def _format_sql_error(error_msg: str, sql: str, columns_info: List[Dict[str, str]]) -> str:
    """Convert SQL errors into user-friendly messages."""
    error_lower = error_msg.lower()

    if "unknown columns" in error_lower or "references unknown columns" in error_lower:
        hints = _friendly_column_hint(columns_info)
        return f"The query mentions columns that aren't in the schema. Available columns: {hints}."

    if "column" in error_lower and ("does not exist" in error_lower or "not found" in error_lower):
        return "One or more columns in your query don't exist. Check the column list and try again."

    if "syntax error" in error_lower:
        return "There was a syntax error in the generated query. Try rephrasing your question."

    if "type" in error_lower and ("mismatch" in error_lower or "cannot" in error_lower):
        return "The query tried to work with incompatible data types. Double-check the columns you are aggregating."

    if "function" in error_lower and ("does not exist" in error_lower or "not found" in error_lower):
        return "The query used a function that isn't available. Try simplifying the operations."

    if "aggregate" in error_lower:
        return "There was an aggregation issue. Make sure the columns used for AVG, COUNT, SUM, etc. are numeric."

    return f"Database error: {error_msg}. If this keeps happening, refine your question or check the column list."


def _convert_sql_to_postgres(sql: str, table_name: str) -> str:
    """Convert DuckDB SQL to PostgreSQL SQL."""
    sql = sql.replace('df', table_name)
    sql = re.sub(r"strftime\('([^']+)', ([^)]+)\)", r"to_char(\2, '\1')", sql)
    sql = sql.replace('DATE_TRUNC', 'date_trunc')
    sql = sql.replace('ILIKE', 'ILIKE')
    return sql


@router.post("/query", response_model=QueryResult)
async def run_query(
    request: QueryRequest,
    session: AsyncSession = Depends(get_db)
):
    logger.info("Query endpoint called")

    global current_dataset, current_columns_info
    dataset_id = request.dataset_id
    dataset_record = None

    if dataset_id:
        dataset_record = await session.get(Dataset, dataset_id)
        if not dataset_record:
            logger.error("Requested dataset %s not found", dataset_id)
            raise HTTPException(status_code=404, detail="Selected dataset not found")
    elif current_dataset is not None:
        dataset_record = current_dataset
    else:
        result = await session.execute(
            select(Dataset).order_by(Dataset.created_at.desc()).limit(1)
        )
        dataset_record = result.scalar_one_or_none()

    if not dataset_record:
        logger.error("No CSV uploaded or available for querying")
        raise HTTPException(status_code=400, detail="No CSV uploaded")

    current_dataset = dataset_record
    current_columns_info = dataset_record.columns_info or []
    logger.info("Using dataset: %s", dataset_record.table_name)

    logger.info("Received data: %s", request.dict())
    question = request.question
    use_ai = request.use_ai if request.use_ai is not None else True
    
    if not question:
        logger.error("Question required")
        raise HTTPException(status_code=400, detail="Question required")

    # 1. Plan Query
    plan = plan_query(question, current_columns_info)
    print("Generated plan:---------------------------", plan)
    logger.info("Query plan: %s", plan.dict())
    
    # 2. Generate SQL
    sql = ""
    columns = [col.get("name") for col in current_columns_info if col.get("name")]
    if use_ai:
        try:
            sql = generate_sql_ai(question, current_columns_info, None)
            logger.info("LLM-generated SQL (raw): %s", sql)
            sql = _convert_sql_to_postgres(sql, dataset_record.table_name)
            sql = validate_sql(sql, columns)
            sql = _rewrite_avg_on_date_columns(sql, current_columns_info)
            _validate_sql_schema(sql, current_columns_info, dataset_record.table_name)
            logger.info("LLM-generated SQL (validated): %s", sql)
        except Exception as e:
            logger.warning(
                "LLM SQL generation failed (%s). Falling back to rule-based SQL.",
                str(e)
            )
            sql = ""

    if not sql:
        sql = generate_sql(plan, dataset_record.table_name, plan.assumptions)
        logger.info("Rule-based SQL: %s", sql)
        if not validate_sql_safety(sql):
            raise HTTPException(status_code=400, detail="Generated SQL failed safety checks")
    
    # 3. Execute
    try:
        raw_rows = await db_service.execute_query(sql, session)
        logger.info("Query executed successfully, rows: %s", len(raw_rows))
        
        rows = []
        if raw_rows:
            df = pd.DataFrame(raw_rows)
            df = df.where(pd.notna(df), None)
            
            # Normalize values
            rows = [
                {key: _normalize_value(value) for key, value in row.items()}
                for row in df.to_dict('records')
            ]
            
        # 4. Construct Response
        # Create a basic answer string
        answer = f"I found {len(rows)} result(s)."
        if plan.time and plan.time.get('grain'):
            answer += f" Filtered by {plan.time['grain']}."

        # Table Preview
        table_preview = None
        if rows:
            table_preview = {
                "columns": list(rows[0].keys()),
                "rows": [list(row.values()) for row in rows[:50]] # Limit preview
            }
            
        # Chart config
        chart = None
        if len(rows) > 0:
            # Simple heuristic for chart
            keys = list(rows[0].keys())
            if len(keys) >= 2:
                # Try to find a label column (string) and a data column (numeric)
                label_col = next((k for k, v in rows[0].items() if isinstance(v, str)), keys[0])
                data_col = next((k for k, v in rows[0].items() if isinstance(v, (int, float))), None)
                
                if data_col:
                    chart_type = "bar"
                    if plan.time: chart_type = "line"
                    
                    chart = {
                        "type": chart_type,
                        "x": label_col,
                        "y": data_col,
                        "title": f"{data_col} by {label_col}"
                    }

        return QueryResult(
            answer=answer,
            assumptions=plan.assumptions,
            sql=sql,
            table_preview=table_preview,
            chart=chart,
            rows=rows # For backward compatibility
        )

    except Exception as e:
        logger.error("Error executing query: %s", str(e))
        user_friendly_error = _format_sql_error(str(e), sql, current_columns_info)
        raise HTTPException(status_code=400, detail=user_friendly_error)
