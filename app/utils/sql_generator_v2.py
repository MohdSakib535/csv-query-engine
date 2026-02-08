from typing import List, Dict, Optional, Any
from app.utils.query_planner_v2 import QueryPlan
import re

def _sanitize_column(col_name: str) -> str:
    """Sanitize column name for SQL."""
    return f'"{col_name}"'

def _sanitize_identifier(identifier: str) -> str:
    """Sanitize strict identifier."""
    # Ensure identifier is safe, only alphanumeric and underscore
    return re.sub(r'[^a-zA-Z0-9_]', '', identifier)

def generate_sql(plan: QueryPlan, table_name: str, assumptions: List[str] = []) -> str:
    sql_parts = ["SELECT"]
    
    # Select Clause
    select_items = []
    if plan.select:
        for col in plan.select:
            select_items.append(_sanitize_column(col))
    
    if plan.aggregations:
        for agg in plan.aggregations:
            col_sql = "*" if agg['col'] == "*" else _sanitize_column(agg['col'])
            fn = agg['fn'].upper()
            select_items.append(f"{fn}({col_sql}) AS {fn.lower()}_{agg['col'].replace('*', 'total').lower()}")
    
    if not select_items:
        select_items = ["*"]
        
    sql_parts.append(", ".join(select_items))
    sql_parts.append(f"FROM {table_name}")
    
    # Where Clause
    where_conditions = []
    if plan.where:
        for cond in plan.where:
            col = _sanitize_column(cond['col'])
            op = cond['op']
            val = cond['value']
            
            # Escape single quotes in strings
            if isinstance(val, str):
                escaped_val = val.replace("'", "''")
                val = f"'{escaped_val}'"
            elif val is None:
                val = 'NULL'
                op = 'IS' if op == '=' else 'IS NOT'
            
            where_conditions.append(f"{col} {op} {val}")
            
    # Time Filters
    if plan.time and 'column' in plan.time:
        time_col = _sanitize_column(plan.time['column'])
        grain = plan.time.get('grain', 'day').lower()
        mode = plan.time.get('mode', 'range').lower()
        
        # Latest logic or range logic
        if mode == 'latest':
           # Subquery to get max date truncated for that grain 
           # Simplistic approach: Just order by date desc limit 1? No, user might want "latest month" data (all rows)
           # Detect if we need to filter by max date
           pass # For now user query planner doesn't output exact time column logic yet. 
                # This needs integration with dataset profile to know WHICH column is time column.

    if where_conditions:
        sql_parts.append("WHERE " + " AND ".join(where_conditions))
        
    # Group By
    if plan.group_by:
        group_cols = [_sanitize_column(col) for col in plan.group_by]
        sql_parts.append("GROUP BY " + ", ".join(group_cols))
        
    # Order By
    if plan.order_by:
        order_items = []
        for order in plan.order_by:
            col = _sanitize_column(order['col'])
            direction = order.get('dir', 'ASC').upper()
            if direction not in ['ASC', 'DESC']: direction = 'ASC'
            order_items.append(f"{col} {direction}")
        sql_parts.append("ORDER BY " + ", ".join(order_items))
        
    # Limit
    if plan.limit:
        try:
            limit_val = int(plan.limit)
            sql_parts.append(f"LIMIT {limit_val}")
        except ValueError:
            pass # Invalid limit ignored

    return " ".join(sql_parts)

def validate_sql_safety(sql: str) -> bool:
    """Ensure SQL is safe read-only query."""
    sql_upper = sql.upper().strip()
    forbidden = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "TRUNCATE", "GRANT", "REVOKE"]
    
    if not sql_upper.startswith("SELECT") and not sql_upper.startswith("WITH"):
         return False

    for word in forbidden:
        # Check word boundary to avoid partial matches
        if re.search(r'\b' + word + r'\b', sql_upper):
            return False
            
    return True
