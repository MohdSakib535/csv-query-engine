from typing import List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.connection import get_db
from app.database.models import Dataset
from app.schemas.models import DatasetSummary, DatasetDetail

router = APIRouter(prefix="/datasets")


@router.get("/", response_model=List[DatasetSummary])
async def list_datasets(session: AsyncSession = Depends(get_db)):
    result = await session.execute(select(Dataset).order_by(Dataset.created_at.desc()))
    datasets = result.scalars().all()
    return [
        DatasetSummary(
            id=dataset.id,
            filename=dataset.filename,
            table_name=dataset.table_name,
            row_count=dataset.row_count,
            columns_count=len(dataset.columns_info or []),
            created_at=dataset.created_at,
        )
        for dataset in datasets
    ]


@router.get("/{dataset_id}", response_model=DatasetDetail)
async def get_dataset(dataset_id: int, session: AsyncSession = Depends(get_db)):
    dataset = await session.get(Dataset, dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    return DatasetDetail(
        id=dataset.id,
        filename=dataset.filename,
        table_name=dataset.table_name,
        row_count=dataset.row_count,
        columns=dataset.columns_info or [],
        created_at=dataset.created_at,
    )
