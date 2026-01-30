from fastapi import APIRouter, HTTPException
import duckdb
from app.routes import upload
from app.utils.sql_generator import generate_sql_rule_based, generate_sql_ai, validate_sql
from app.schemas.models import QueryRequest, QueryResult
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter()

@router.post("/query", response_model=QueryResult)
async def run_query(request: QueryRequest):
    logger.info("Query endpoint called")
    if upload.uploaded_df is None:
        print("uploadeddf -------------", upload.uploaded_df)
        logger.error("No CSV uploaded")
        raise HTTPException(status_code=400, detail="No CSV uploaded")

    logger.info(f"Received data: {request.dict()}")
    question = request.question
    use_ai = request.use_ai if request.use_ai is not None else True

    if not question:
        logger.error("Question required")
        raise HTTPException(status_code=400, detail="Question required")

    columns = [col['name'] for col in upload.columns_info]
    logger.info(f"Columns: {columns}")

    try:
        if use_ai:
            sql = generate_sql_ai(question, upload.columns_info)
        else:
            sql = generate_sql_rule_based(question, upload.columns_info)
        logger.info(f"Generated SQL: {sql}")

        sql = validate_sql(sql, columns)
        logger.info(f"Validated SQL: {sql}")

        con = duckdb.connect()
        con.register('df', upload.uploaded_df)
        result = con.execute(sql).fetchdf()
        con.close()
        logger.info(f"Query executed successfully, rows: {len(result)}")

        rows = result.to_dict('records')
        return QueryResult(sql=sql, rows=rows)
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Error executing query: {str(e)}")
