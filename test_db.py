import asyncio
import os
from app.database.connection import get_db, create_tables

async def test_db():
    try:
        print("Testing database connection...")
        async for session in get_db():
            print("Database connection successful!")
            # Try to execute a simple query
            result = await session.execute("SELECT 1 as test")
            row = result.fetchone()
            print(f"Test query result: {row}")
            break

        print("Creating tables...")
        await create_tables()
        print("Tables created successfully!")

    except Exception as e:
        print(f"Database error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_db())