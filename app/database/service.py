import pandas as pd
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import text
import uuid
from typing import List, Dict, Any
from app.database.models import Dataset, DatasetSession

class DatabaseService:
    def __init__(self):
        self.current_session_id = None
        self.current_dataset = None

    async def create_dynamic_table(self, df: pd.DataFrame, table_name: str, session: AsyncSession):
        """Create a dynamic table based on DataFrame schema"""
        columns = []
        column_mapping = {}  # Store original -> cleaned mapping

        for col_name, dtype in df.dtypes.items():
            # Clean column name for PostgreSQL
            clean_col_name = col_name.lower().replace(' ', '_').replace('-', '_')
            column_mapping[col_name] = clean_col_name

            if pd.api.types.is_integer_dtype(dtype):
                col_type = "BIGINT"
            elif pd.api.types.is_float_dtype(dtype):
                col_type = "FLOAT"
            elif pd.api.types.is_bool_dtype(dtype):
                col_type = "BOOLEAN"
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                col_type = "TIMESTAMP"
            else:
                # Check if it's mostly numeric strings that should be treated as text
                sample_values = df[col_name].dropna().head(100)
                if len(sample_values) > 0 and sample_values.astype(str).str.match(r'^\d+$').any():
                    col_type = "TEXT"
                else:
                    col_type = "TEXT"

            columns.append(f'"{clean_col_name}" {col_type}')

        # Add primary key
        columns.insert(0, 'id SERIAL PRIMARY KEY')

        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(columns)}
        );
        """

        await session.execute(text(create_table_sql))
        await session.commit()

        return column_mapping

    async def insert_dataframe(self, df: pd.DataFrame, table_name: str, session: AsyncSession):
        """Insert DataFrame data into the table"""
        if df.empty:
            return

        # Convert DataFrame to records
        records = df.to_dict('records')

        # Build INSERT statement
        columns = list(df.columns)
        placeholders = ', '.join([f':{col}' for col in columns])
        columns_str = ', '.join([f'"{col}"' for col in columns])

        insert_sql = f"""
        INSERT INTO {table_name} ({columns_str})
        VALUES ({placeholders})
        """

        # Execute in batches to avoid memory issues
        batch_size = 1000
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            await session.execute(text(insert_sql), batch)

        await session.commit()

    async def store_csv_data(self, df: pd.DataFrame, filename: str, columns_info: List[Dict], session: AsyncSession) -> Dataset:
        """Store CSV data in database and return dataset info"""
        # Generate unique table name
        table_name = f"dataset_{uuid.uuid4().hex[:16]}"

        # Create dynamic table
        column_mapping = await self.create_dynamic_table(df, table_name, session)

        # Rename DataFrame columns to match cleaned names
        df = df.rename(columns=column_mapping)

        # Update columns_info to use cleaned column names
        updated_columns_info = []
        for col_info in columns_info:
            original_name = col_info['name']
            cleaned_name = column_mapping.get(original_name, original_name)
            updated_col_info = col_info.copy()
            updated_col_info['name'] = cleaned_name
            updated_col_info['original_name'] = original_name  # Keep original for display
            updated_columns_info.append(updated_col_info)

        # Insert data
        await self.insert_dataframe(df, table_name, session)

        # Create dataset record
        dataset = Dataset(
            filename=filename,
            table_name=table_name,
            columns_info=updated_columns_info,
            row_count=len(df)
        )

        session.add(dataset)
        await session.commit()
        await session.refresh(dataset)

        return dataset

    async def execute_query(self, sql: str, session: AsyncSession) -> List[Dict[str, Any]]:
        """Execute a SQL query and return results"""
        result = await session.execute(text(sql))
        rows = result.fetchall()

        # Convert to dict format
        if rows:
            columns = result.keys()
            return [dict(zip(columns, row)) for row in rows]
        return []

    async def get_table_info(self, table_name: str, session: AsyncSession) -> Dict[str, Any]:
        """Get table information"""
        # Get column information
        columns_query = f"""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position;
        """

        columns_result = await session.execute(text(columns_query))
        columns = columns_result.fetchall()

        # Get row count
        count_query = f"SELECT COUNT(*) FROM {table_name};"
        count_result = await session.execute(text(count_query))
        row_count = count_result.scalar()

        return {
            "columns": [{"name": col[0], "type": col[1], "nullable": col[2]} for col in columns],
            "row_count": row_count
        }

    async def drop_table(self, table_name: str, session: AsyncSession):
        """Drop a table"""
        await session.execute(text(f"DROP TABLE IF EXISTS {table_name};"))
        await session.commit()

# Global instance
db_service = DatabaseService()