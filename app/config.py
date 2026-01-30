import os
from typing import Optional

class Config:
    OPENAI_API_KEY: Optional[str] = os.getenv("OPENAI_API_KEY")
    OPENAI_MODEL: str = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    DATABASE_URL: Optional[str] = os.getenv("DATABASE_URL")  # If needed for future extensions
    DEBUG: bool = os.getenv("DEBUG", "False").lower() == "true"
    MAX_ROWS_LIMIT: int = int(os.getenv("MAX_ROWS_LIMIT", "200"))

config = Config()
