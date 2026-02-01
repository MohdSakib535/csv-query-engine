
from app.utils.query_planner import build_query_plan

def test_planner():
    question = "List incidents where duration was more than 60 minutes."
    
    # Scene 1: Column exists exactly
    columns_exact = [{"name": "duration"}]
    plan = build_query_plan(question, columns_exact)
    print(f"Exact match ('duration'): Vague? {plan.is_vague()}")
    
    # Scene 2: Column is snake_case with duration
    columns_snake = [{"name": "incident_duration"}]
    plan = build_query_plan(question, columns_snake)
    print(f"Partial match ('incident_duration'): Vague? {plan.is_vague()}")

    # Scene 3: Column is duration_seconds
    columns_suffix = [{"name": "duration_seconds"}]
    plan = build_query_plan(question, columns_suffix)
    print(f"Partial match ('duration_seconds'): Vague? {plan.is_vague()}")
    
    # Scene 4: Plural check
    question_plural = "Show details for issues"
    columns_singular = [{"name": "issue"}]
    plan = build_query_plan(question_plural, columns_singular)
    print(f"Plural match ('issues' vs 'issue'): Vague? {plan.is_vague()}")

if __name__ == "__main__":
    test_planner()
