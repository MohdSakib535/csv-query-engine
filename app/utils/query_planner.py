from dataclasses import dataclass, field
import re
from typing import Dict, List, Iterable

AGGREGATION_KEYWORDS = [
    "average", "avg", "mean", "sum", "total", "count", "ratio", "percentage",
    "max", "min", "median", "distinct", "distribution", "trend"
]
SORT_KEYWORDS = [
    "top", "highest", "lowest", "bottom", "ordered", "order by", "sort by",
    "recent", "earliest"
]


@dataclass
class QueryPlan:
    referenced_columns: List[str] = field(default_factory=list)
    filters: List[str] = field(default_factory=list)
    aggregations: List[str] = field(default_factory=list)
    sorts: List[str] = field(default_factory=list)

    def is_vague(self) -> bool:
        return not (
            self.referenced_columns
            or self.filters
            or self.aggregations
            or self.sorts
        )

    def describe(self, columns_info: Iterable[Dict]) -> str:
        column_map = {col["name"]: col for col in columns_info}
        friendly_columns = []
        for name in self.referenced_columns:
            col = column_map.get(name)
            if col:
                friendly_columns.append(col.get("original_name") or col["name"])

        sections = [
            f"- Columns referenced: {', '.join(friendly_columns) if friendly_columns else 'none'}",
            f"- Filters: {', '.join(self.filters) if self.filters else 'none'}",
            f"- Aggregations: {', '.join(self.aggregations) if self.aggregations else 'none'}",
            f"- Sorting: {', '.join(self.sorts) if self.sorts else 'none'}",
        ]
        return "\n".join(sections)


def _column_aliases(column: Dict) -> Iterable[str]:
    normalized = column.get("name", "")
    aliases = {normalized.lower()}
    if normalized:
        aliases.add(normalized.replace("_", " ").lower())
        aliases.add(normalized.replace("_", "").lower())

    original = column.get("original_name")
    if original:
        original_lower = original.lower()
        aliases.add(original_lower)
        aliases.add(original_lower.replace(" ", "").lower())
        aliases.add(re.sub(r"\s+", " ", original_lower))
        camel_split = re.sub(r"(?<!^)(?=[A-Z])", " ", original).lower()
        aliases.add(camel_split)

    return {alias for alias in aliases if alias}


def _build_column_lookup(columns_info: Iterable[Dict]) -> Dict[str, List[str]]:
    lookup = {}
    for column in columns_info:
        normalized = column.get("name")
        if not normalized:
            continue
        lookup[normalized] = list(_column_aliases(column))
    return lookup


def _match_variants(question: str, variants: Iterable[str]) -> bool:
    lower = question.lower()
    for variant in variants:
        if not variant:
            continue
        pattern = re.compile(rf"\b{re.escape(variant)}\b", re.IGNORECASE)
        if pattern.search(lower):
            return True
    return False


def build_query_plan(question: str, columns_info: Iterable[Dict]) -> QueryPlan:
    plan = QueryPlan()
    if not question:
        return plan

    column_lookup = _build_column_lookup(columns_info)
    seen = set()
    for normalized, variants in column_lookup.items():
        if normalized in seen:
            continue
        if _match_variants(question, variants):
            plan.referenced_columns.append(normalized)
            seen.add(normalized)

    lower = question.lower()
    between_match = re.search(r"between\s+([^\s,]+)\s+and\s+([^\s,]+)", lower)
    if between_match:
        start, end = between_match.groups()
        plan.filters.append(f"between {start} and {end}")

    range_match = re.search(r"from\s+([^\s,]+)\s+to\s+([^\s,]+)", lower)
    if range_match:
        start, end = range_match.groups()
        plan.filters.append(f"from {start} to {end}")

    if re.search(r"\b(after|before|since|until|during|last|past|recent)\b", lower):
        plan.filters.append("relative time or range filter mentioned")

    detected_aggs = []
    for keyword in AGGREGATION_KEYWORDS:
        if re.search(rf"\b{re.escape(keyword)}\b", lower):
            detected_aggs.append(keyword)
    if detected_aggs:
        plan.aggregations.extend(dict.fromkeys(detected_aggs))

    detected_sorts = []
    for keyword in SORT_KEYWORDS:
        if keyword in lower:
            detected_sorts.append(keyword)
    if "order by" in lower:
        detected_sorts.append("order by clause requested")
    if detected_sorts:
        plan.sorts.extend(dict.fromkeys(detected_sorts))

    return plan
