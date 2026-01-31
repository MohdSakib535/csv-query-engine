from fastapi import APIRouter, File, UploadFile, HTTPException, Depends
from sqlalchemy.ext.asyncio import AsyncSession
import pandas as pd
from app.utils.csv_profiler import profile_csv
from app.schemas.models import UploadResponse
from app.database.connection import get_db
from app.database.service import db_service
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter()

# Global to store current dataset info (for backward compatibility with frontend)
current_dataset = None
current_columns_info = []

@router.post("/upload", response_model=UploadResponse)
async def upload_file(
    file: UploadFile = File(...),
    session: AsyncSession = Depends(get_db)
):
    global current_dataset, current_columns_info

    logger.info(f"Uploading file: {file.filename}")
    if not file.filename.endswith('.csv'):
        logger.error("Only CSV files allowed")
        raise HTTPException(status_code=400, detail="Only CSV files allowed")

    try:
        # Read CSV
        df = pd.read_csv(file.file)

        # Profile columns
        columns_info = profile_csv(df)

        # Cast detected date columns to datetime
        for col in columns_info:
            if col.get("semantic_type") == "date":
                col_name = col.get("name")
                if col_name in df.columns:
                    df[col_name] = pd.to_datetime(df[col_name], errors="coerce", format="mixed")

        # Store in database
        dataset = await db_service.store_csv_data(df, file.filename, columns_info, session)

        # Update global state for current session
        current_dataset = dataset
        # Use original column names for frontend display
        current_columns_info = [
            {**col, 'name': col.get('original_name', col['name'])}
            for col in dataset.columns_info
        ]

        logger.info(f"File uploaded successfully, shape: {df.shape}, columns: {len(columns_info)}, table: {dataset.table_name}")
        return UploadResponse(columns=current_columns_info)

    except Exception as e:
        logger.error(f"Error reading CSV: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Error reading CSV: {str(e)}")
