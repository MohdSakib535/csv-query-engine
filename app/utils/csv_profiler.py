import pandas as pd
from typing import Dict, List

def detect_column_type(col_name: str, sample_values: pd.Series) -> str:
    """Detect semantic column type based on name and content."""
    col_lower = col_name.lower()

    # City detection
    city_keywords = ['city', 'location', 'place', 'town', 'state', 'country', 'region', 'area']
    if any(keyword in col_lower for keyword in city_keywords):
        return 'city'

    # Service/Product detection
    service_keywords = ['service', 'product', 'type', 'category', 'item', 'name', 'title']
    if any(keyword in col_lower for keyword in service_keywords):
        return 'service'

    # Date detection
    date_keywords = ['date', 'time', 'timestamp', 'created', 'updated', 'occurred']
    if any(keyword in col_lower for keyword in date_keywords):
        return 'date'

    # Check if column contains datetime-like data
    try:
        pd.to_datetime(sample_values.head(), errors='coerce', format='mixed')
        if sample_values.head().notna().any():
            return 'date'
    except:
        pass

    return 'other'

def profile_csv(df: pd.DataFrame) -> List[Dict]:
    columns = []

    for col in df.columns:
        dtype = str(df[col].dtype)
        inferred_type = "string"
        semantic_type = detect_column_type(col, df[col])

        if dtype.startswith('int') or dtype.startswith('float'):
            inferred_type = "numeric"
        elif dtype == 'object':
            # Check if looks like date
            try:
                pd.to_datetime(df[col].head(), errors='coerce', format='mixed')
                inferred_type = "date"
                semantic_type = 'date'
            except:
                inferred_type = "string"
        elif dtype.startswith('datetime'):
            inferred_type = "date"
            semantic_type = 'date'

        columns.append({
            "name": col,
            "type": inferred_type,
            "semantic_type": semantic_type
        })

    return columns
