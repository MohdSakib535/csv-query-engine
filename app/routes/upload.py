from fastapi import APIRouter, File, UploadFile, HTTPException, Depends
from sqlalchemy.ext.asyncio import AsyncSession
import pandas as pd
from app.utils.csv_profiler import profile_csv
from app.utils.csv_schema import normalize_dataframe_headers
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

        # Normalize headers before profiling
        df, _, normalized_to_original = normalize_dataframe_headers(df)
        dtype_lookup = {col: str(df[col].dtype) for col in df.columns}

        # Profile columns
        columns_info = profile_csv(df)

        for col_info in columns_info:
            normalized = col_info["name"]
            col_info["original_name"] = normalized_to_original.get(normalized, normalized)
            col_info["data_type"] = dtype_lookup.get(normalized, "unknown")

        # Cast detected date columns to datetime
        for col in columns_info:
            if col.get("semantic_type") == "date":
                col_name = col.get("name")
                if col_name in df.columns:
                    df[col_name] = pd.to_datetime(df[col_name], errors="coerce", format="mixed")

        # Store in database
        dataset = await db_service.store_csv_data(df, file.filename, columns_info, session)

        # Update global state for current session (normalized names are stored in dataset)
        current_dataset = dataset
        current_columns_info = dataset.columns_info

        response_columns = []
        for col in dataset.columns_info:
            response_columns.append({
                "name": col.get("name"),
                "type": col.get("type"),
                "semantic_type": col.get("semantic_type"),
                "data_type": col.get("data_type"),
                "original_name": col.get("original_name"),
                "database_name": col.get("name"),
            })

        logger.info(f"File uploaded successfully, shape: {df.shape}, columns: {len(columns_info)}, table: {dataset.table_name}")
        return UploadResponse(
            dataset_id=dataset.id,
            table_name=dataset.table_name,
            filename=dataset.filename,
            row_count=dataset.row_count,
            columns=response_columns
        )

    except Exception as e:
        logger.error(f"Error reading CSV: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Error reading CSV: {str(e)}")


@router.delete("/upload/{dataset_id}")
async def delete_uploaded_dataset(
    dataset_id: int,
    session: AsyncSession = Depends(get_db)
):
    global current_dataset, current_columns_info

    dataset = await db_service.delete_dataset(dataset_id, session)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    if current_dataset is not None and current_dataset.id == dataset_id:
        current_dataset = None
        current_columns_info = []

    return {"detail": "Dataset deleted"}
