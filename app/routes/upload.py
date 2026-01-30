from fastapi import APIRouter, File, UploadFile, HTTPException
import pandas as pd
from app.utils.csv_profiler import profile_csv
from app.schemas.models import UploadResponse
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter()

# Global to store the uploaded DataFrame
uploaded_df = None
columns_info = []

@router.post("/upload", response_model=UploadResponse)
async def upload_file(file: UploadFile = File(...)):
    global uploaded_df, columns_info
    logger.info(f"Uploading file: {file.filename}")
    if not file.filename.endswith('.csv'):
        logger.error("Only CSV files allowed")
        raise HTTPException(status_code=400, detail="Only CSV files allowed")

    try:
        df = pd.read_csv(file.file)
        uploaded_df = df
        columns_info = profile_csv(df)
        # Cast detected date columns to datetime so DuckDB sees proper types
        for col in columns_info:
            if col.get("semantic_type") == "date":
                col_name = col.get("name")
                if col_name in df.columns:
                    df[col_name] = pd.to_datetime(df[col_name], errors="coerce", format="mixed")
        logger.info(f"File uploaded successfully, shape: {df.shape}, columns: {len(columns_info)}")
        return UploadResponse(columns=columns_info)
    except Exception as e:
        logger.error(f"Error reading CSV: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Error reading CSV: {str(e)}")
