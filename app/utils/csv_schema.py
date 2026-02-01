import re
from typing import Dict, Tuple

import pandas as pd

_NON_ALPHANUMERIC = re.compile(r'[^a-z0-9]+')


def _normalize_name(base: str) -> str:
    cleaned = _NON_ALPHANUMERIC.sub('_', base.strip().lower())
    cleaned = re.sub(r'_+', '_', cleaned).strip('_')
    return cleaned or "column"


def normalize_dataframe_headers(
    df: pd.DataFrame
) -> Tuple[pd.DataFrame, Dict[str, str], Dict[str, str]]:
    """
    Normalize the column headers to lowercase, underscore separated names and return mapping helpers.
    """
    mapping: Dict[str, str] = {}
    normalized_to_original: Dict[str, str] = {}
    counters: Dict[str, int] = {}

    for original_name in list(df.columns):
        normalized = _normalize_name(original_name)
        base = normalized
        duplicate_index = counters.get(base, 0)
        if duplicate_index:
            normalized = f"{base}_{duplicate_index}"
        counters[base] = duplicate_index + 1
        mapping[original_name] = normalized
        normalized_to_original[normalized] = original_name

    df = df.rename(columns=mapping)
    return df, mapping, normalized_to_original
