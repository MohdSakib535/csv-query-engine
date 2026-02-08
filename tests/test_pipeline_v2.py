
import pytest
import pandas as pd
from app.utils.csv_profiler import profile_csv, detect_column_type
from app.utils.query_planner_v2 import plan_query, QueryPlan
from app.utils.sql_generator_v2 import generate_sql, validate_sql_safety

# --- Profiler Tests ---
def test_profiler_stats():
    data = {
        'City': ['Mumbai', 'Delhi', 'Mumbai', None, 'Pune'],
        'Age': [25, 30, 35, 40, 45],
        'Date': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05'])
    }
    df = pd.DataFrame(data)
    profile = profile_csv(df)
    
    city_col = next(c for c in profile if c['name'] == 'City')
    assert city_col['null_ratio'] == 0.2
    assert city_col['distinct_count'] == 3
    assert 'Mumbai' in city_col['top_values']
    assert city_col['semantic_type'] == 'geo'

    age_col = next(c for c in profile if c['name'] == 'Age')
    assert age_col['min'] == 25
    assert age_col['max'] == 45
    assert age_col['type'] == 'numeric'

# --- Planner Tests ---
def test_planner_entity_resolution():
    columns_info = [
        {'name': 'City', 'top_values': ['New Delhi', 'Mumbai', 'Bangalore'], 'type': 'string'}
    ]
    # "New Delhi" should match "City" column
    plan = plan_query("incidents in New Delhi", columns_info)
    assert len(plan.where) == 1
    assert plan.where[0]['col'] == 'City'
    assert plan.where[0]['value'] == 'New Delhi'

def test_planner_time_grain():
    columns_info = [{'name': 'Date', 'type': 'date'}]
    plan = plan_query("monthly incidents", columns_info)
    assert plan.time['grain'] == 'month'

def test_planner_fuzzy_match():
    columns_info = [
        {'name': 'Service', 'top_values': ['Internet Connectivity', 'Power Supply'], 'type': 'string'}
    ]
    # "Internet" should match "Internet Connectivity"
    plan = plan_query("issues with Internet", columns_info)
    assert plan.where[0]['col'] == 'Service'
    assert plan.where[0]['value'] == 'Internet Connectivity'

# --- SQL Generator Tests ---
def test_sql_generation_basic():
    plan = QueryPlan(
        select=['City', 'Service'],
        where=[{'col': 'City', 'op': '=', 'value': 'Mumbai'}],
        limit=10
    )
    sql = generate_sql(plan, 'my_table')
    assert 'SELECT "City", "Service" FROM my_table' in sql
    assert 'WHERE "City" = \'Mumbai\'' in sql
    assert 'LIMIT 10' in sql

def test_sql_safety():
    assert validate_sql_safety("SELECT * FROM table") == True
    assert validate_sql_safety("DROP TABLE table") == False
    assert validate_sql_safety("SELECT * FROM table; DELETE FROM table") == False

if __name__ == "__main__":
    # fast manual run
    test_profiler_stats()
    test_planner_entity_resolution()
    test_planner_time_grain()
    test_planner_fuzzy_match()
    test_sql_generation_basic()
    test_sql_safety()
    print("All tests passed!")
