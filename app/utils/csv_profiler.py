import pandas as pd
from typing import Dict, List
from datetime import datetime, date

def _json_safe(value):
    if value is None or pd.isna(value):
        return None
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return value

def detect_column_type(col_name: str, sample_values: pd.Series) -> str:
    """Detect semantic column type based on name and content."""
    col_lower = col_name.lower().strip()

    # Geo/Location detection
    geo_keywords = ['city', 'location', 'place', 'town', 'state', 'country', 'region', 'area', 'zip', 'postal', 'address']
    if any(keyword in col_lower for keyword in geo_keywords):
        return 'geo'

    # Entity/Product detection
    entity_keywords = ['service', 'product', 'item', 'name', 'title', 'brand', 'model', 'team', 'customer', 'client', 'user']
    if any(keyword in col_lower for keyword in entity_keywords):
        return 'entity'
    
    # Category/Type detection
    category_keywords = ['type', 'category', 'status', 'priority', 'level', 'group', 'class', 'genre']
    if any(keyword in col_lower for keyword in category_keywords):
        return 'category'

    # Date detection
    date_keywords = ['date', 'time', 'timestamp', 'created', 'updated', 'occurred', 'reported', 'closed', 'start', 'end']
    if any(keyword in col_lower for keyword in date_keywords):
        return 'date'
    
    # If no keyword match, check if content is datetime
    if pd.api.types.is_datetime64_any_dtype(sample_values):
         return 'date'

    return 'other'

def profile_csv(df: pd.DataFrame) -> List[Dict]:
    columns = []

    for col in df.columns:
        series = df[col]
        dtype = str(series.dtype)
        inferred_type = "string"
        semantic_type = "other"
        
        # Calculate base stats
        row_count = len(series)
        null_count = series.isna().sum()
        null_ratio = null_count / row_count if row_count > 0 else 0
        distinct_count = series.nunique()
        
        # Initialize stats
        stats = {
            "null_ratio": float(null_ratio),
            "distinct_count": int(distinct_count),
            "top_values": [],
            "min": None,
            "max": None,
            "mean": None,
            "min_date": None,
            "max_date": None
        }

        # Type Inference Logic
        if pd.api.types.is_numeric_dtype(series):
            inferred_type = "numeric"
            stats["min"] = float(series.min()) if not series.empty else None
            stats["max"] = float(series.max()) if not series.empty else None
            stats["mean"] = float(series.mean()) if not series.empty else None
            
        elif pd.api.types.is_datetime64_any_dtype(series):
            inferred_type = "date"
            semantic_type = "date"
            stats["min_date"] = _json_safe(series.min()) if not series.empty else None
            stats["max_date"] = _json_safe(series.max()) if not series.empty else None
            
        else: # Object/String
             # Check if looks like date
            try:
                # parsing with errors='coerce' to check success rate
                parsed = pd.to_datetime(series.head(100), errors='coerce', format='mixed')
                valid_ratio = parsed.notna().mean()
                
                if valid_ratio >= 0.8: # High confidence date
                    inferred_type = 'date'
                    semantic_type = 'date'
                    # Convert whole column for accurate stats
                    temp_dates = pd.to_datetime(series, errors='coerce', format='mixed')
                    stats["min_date"] = _json_safe(temp_dates.min()) if not temp_dates.empty else None
                    stats["max_date"] = _json_safe(temp_dates.max()) if not temp_dates.empty else None
                else: 
                     inferred_type = "string"
                     # Get top values for categorical processing
                     top_vals = series.value_counts().head(20).index.tolist()
                     stats["top_values"] = [str(v) for v in top_vals]
                     
                     semantic_type = detect_column_type(col, series)

            except Exception:
                inferred_type = "string"
                top_vals = series.value_counts().head(20).index.tolist()
                stats["top_values"] = [str(v) for v in top_vals]
                semantic_type = detect_column_type(col, series)

        columns.append({
            "name": col,
            "type": inferred_type,
            "semantic_type": semantic_type,
            **stats
        })

    return columns
