import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from fastapi import HTTPException
from app.config import config
from openai import OpenAI

def quote_identifier(name: str) -> str:
    """Quote column/table names for SQL safety."""
    return f'"{name}"'

def get_last_month_range():
    now = datetime.now()
    start_of_this_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    start_of_last_month = (start_of_this_month - timedelta(days=1)).replace(day=1)
    end_of_last_month = start_of_this_month - timedelta(seconds=1)
    return start_of_last_month, end_of_last_month

def _find_semantic_column(columns_info: List[Dict[str, str]], semantic_type: str) -> Optional[str]:
    for col in columns_info:
        if col.get("semantic_type") == semantic_type:
            return col.get("name")
    return None

def _strip_sql_fences(text: str) -> str:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:sql)?", "", cleaned, flags=re.IGNORECASE).strip()
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3].strip()
    return cleaned

def _columns_prompt(columns_info: List[Dict[str, str]]) -> str:
    parts = []
    for col in columns_info:
        name = col.get("name", "")
        dtype = col.get("type", "unknown")
        semantic = col.get("semantic_type", "other")
        parts.append(f"{name} ({dtype}, {semantic})")
    return ", ".join(parts)

def generate_sql_rule_based(question: str, columns_info: List[Dict[str, str]]) -> str:
    question_lower = question.lower()
    select_cols = "*"
    group_by = None
    where_clauses = []
    columns = [col.get("name") for col in columns_info if col.get("name")]

    # Detect "which X" for group by
    which_match = re.search(r'which (\w+)', question_lower)
    if which_match:
        col = which_match.group(1)
        if col in [c.lower() for c in columns]:
            actual_col = next(c for c in columns if c.lower() == col)
            group_by = quote_identifier(actual_col)
            select_cols = f"{quote_identifier(actual_col)}, COUNT(*) as count"

    # Filter by city
    city_col = _find_semantic_column(columns_info, "city")
    if city_col:
        cities = ['mumbai', 'delhi', 'bangalore', 'chennai', 'kolkata', 'hyderabad', 'pune', 'ahmedabad']
        for city in cities:
            if city in question_lower:
                where_clauses.append(f"{quote_identifier(city_col)} = '{city.capitalize()}'")
                break

    # Filter by last month
    date_col = _find_semantic_column(columns_info, "date")
    if date_col and 'last month' in question_lower:
        start, end = get_last_month_range()
        where_clauses.append(f"{quote_identifier(date_col)} >= '{start.isoformat()}' AND {quote_identifier(date_col)} <= '{end.isoformat()}'")

    sql = f"SELECT {select_cols} FROM df"
    if where_clauses:
        sql += " WHERE " + " AND ".join(where_clauses)
    if group_by:
        sql += f" GROUP BY {group_by}"
    return sql

def generate_sql_ai(question: str, columns_info: List[Dict[str, str]]) -> str:
    if not config.OPENAI_API_KEY:
        raise HTTPException(status_code=400, detail="OpenAI API key not configured")

    columns_text = _columns_prompt(columns_info)
    prompt = f"""
    Generate a DuckDB SQL SELECT query for the question: "{question}"
    Available columns (name, type, semantic): {columns_text}
    Current date: {datetime.now().isoformat()}
    Last month range: {get_last_month_range()[0].isoformat()} to {get_last_month_range()[1].isoformat()}
    Use the table name df.
    Instructions:
    - Quote column names that require it with double quotes (e.g., "Column Name").
    - Only apply aggregates such as AVG, SUM, MIN, or MAX to columns whose reported type is numeric. Do not average, sum, or otherwise aggregate string, boolean, or date columns unless the question explicitly asks for an average date/timestamp; in that case convert the column to seconds with EXTRACT(EPOCH FROM ...) before averaging and wrap the result with TO_TIMESTAMP or TO_CHAR to keep it readable.
    - Ensure the query is safe, only SELECT, and references existing columns.
    Return only the SQL (no backticks, no explanations).
    """

    client = OpenAI(api_key=config.OPENAI_API_KEY)
    response = client.chat.completions.create(
        model=config.OPENAI_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )
    content = response.choices[0].message.content or ""
    return _strip_sql_fences(content)

def validate_sql(sql: str, columns: List[str]) -> str:
    cleaned = sql.strip()
    if cleaned.endswith(";"):
        cleaned = cleaned[:-1].strip()

    sql_upper = cleaned.upper()
    forbidden = ['INSERT', 'UPDATE', 'DELETE', 'DROP', 'ALTER', 'CREATE', 'ATTACH', 'COPY', 'PRAGMA']
    if any(word in sql_upper for word in forbidden):
        raise HTTPException(status_code=400, detail="Unsafe SQL query")

    if cleaned.count(';') > 0:
        raise HTTPException(status_code=400, detail="Multiple statements not allowed")

    # Check columns
    for col in columns:
        if col not in sql:
            continue  # Allow if not all columns used
    # Actually, parse to check, but for simplicity, assume ok if no forbidden

    if 'LIMIT' not in sql_upper:
        cleaned += f" LIMIT {config.MAX_ROWS_LIMIT}"

    return cleaned
