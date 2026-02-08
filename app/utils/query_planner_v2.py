from typing import List, Dict, Optional, Any
from pydantic import BaseModel
from datetime import datetime
import re
from rapidfuzz import process, fuzz

class QueryPlan(BaseModel):
    select: List[str] = []
    where: List[Dict[str, Any]] = []  # {"col": "...", "op": "=", "value": "..."}
    group_by: List[str] = []
    aggregations: List[Dict[str, str]] = []  # [{"fn":"count", "col":"Service"}]
    order_by: List[Dict[str, str]] = []
    limit: int = 50
    time: Optional[Dict[str, str]] = None # {"grain": "month", "mode": "latest"}
    assumptions: List[str] = []

def _normalize(text: str) -> str:
    return str(text).lower().strip()

def _find_matching_column(token: str, columns_info: List[Dict], threshold=85) -> Optional[str]:
    choices = [col['name'] for col in columns_info]
    # token_sort_ratio helps with reordered words, but ratio is fine for simple names
    match = process.extractOne(token, choices, scorer=fuzz.token_sort_ratio)
    if match and match[1] >= threshold:
        return match[0]
    return None

def _find_value_match(token: str, columns_info: List[Dict], threshold=75) -> Optional[Dict]:
    """Find which column a value likely belongs to."""
    best_match = None
    best_score = 0
    
    for col in columns_info:
        top_vals = col.get('top_values', [])
        if not top_vals: 
            continue
        
        # token_set_ratio is very good for partial matches and finding "Internet" in "Internet Connectivity"
        match = process.extractOne(token, top_vals, scorer=fuzz.token_set_ratio)
        if match and match[1] > best_score:
            best_score = match[1]
            best_match = {
                "col": col['name'],
                "value": match[0],
                "score": match[1]
            }
            
    if best_match and best_match['score'] >= threshold:
        return best_match
    return None

def plan_query(question: str, columns_info: List[Dict]) -> QueryPlan:
    plan = QueryPlan()
    
    # 1. Detect Time Intent
    question_lower = question.lower()
    time_grain = None
    time_mode = None
    
    if "latest" in question_lower or "last" in question_lower:
        time_mode = "latest"
    
    if "month" in question_lower:
        time_grain = "month"
    elif "quarter" in question_lower:
        time_grain = "quarter"
    elif "year" in question_lower:
        time_grain = "year"
    elif "week" in question_lower:
        time_grain = "week"
        
    if time_grain:
        plan.time = {"grain": time_grain, "mode": time_mode or "range"}

    # 2. Extract potential entities and columns (Heuristic based)
    # Generate n-grams to catch multi-word entities/columns (up to 3 words)
    tokens = [t for t in re.split(r'\s+', question) if t]
    ngrams = []
    
    # Create ngrams of size 3, 2, 1
    for n in range(3, 0, -1):
        for i in range(len(tokens) - n + 1):
            gram = " ".join(tokens[i:i+n])
            # Filter out short stopwords only if it's a 1-gram
            if n == 1:
                if gram.lower() in ["show", "me", "list", "give", "the", "of", "in", "by", "for", "with", "a", "an", "is", "are", "what", "how", "many"]:
                    continue
                if len(gram) < 2: continue
            ngrams.append(gram)
    
    # Process ngrams - prioritize longer matches
    matched_indices = set() # Track parts of question already used
    
    for gram in ngrams:
        # Check if matched already (simplified check)
        # In a full implementation we'd track token indices. 
        # For now, let's just try to match.
        
        # Check if token is a value in any column
        val_match = _find_value_match(gram, columns_info)
        if val_match:
            # Check if we already have a filter for this column
            exists = any(f['col'] == val_match['col'] for f in plan.where)
            if not exists:
                plan.where.append({
                    "col": val_match['col'],
                    "op": "=",
                    "value": val_match['value']
                })
                plan.assumptions.append(f"Assumed '{gram}' refers to {val_match['col']} = '{val_match['value']}'")
                continue

        # Check if token is a column name
        col_match = _find_matching_column(gram, columns_info)
        if col_match:
             if col_match not in plan.group_by and col_match not in plan.select:
                 plan.select.append(col_match)

    # 3. Detect Aggregations
    if any(w in question_lower for w in ["count", "number of", "how many"]):
        if not plan.aggregations:
             plan.aggregations.append({"fn": "count", "col": "*"})
    
    # If grouping but no metric, add count
    if (plan.group_by or plan.select) and not plan.aggregations:
         # Implicit count if looking for distribution
         if "breakdown" in question_lower or "per" in question_lower or "by" in question_lower:
             plan.aggregations.append({"fn": "count", "col": "*"})
    

    # 4. Final adjustments
    # If we have a WHERE clause but no SELECT, select all or specific columns
    if plan.where and not plan.select:
        # Select columns that are NOT in the where clause to show context
        for col in columns_info:
            if col['name'] not in [w['col'] for w in plan.where]:
                plan.select.append(col['name'])
                if len(plan.select) >= 3: break
    
    if not plan.select and not plan.aggregations:
        plan.select = [col['name'] for col in columns_info[:5]] # Default select top 5 cols

    return plan
