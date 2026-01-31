from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from app.routes.upload import router as upload_router
from app.routes.query import router as query_router
from app.database.connection import create_tables, DATABASE_URL
import logging
import asyncio

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="CSV Q&A Analytics", description="AI-powered CSV data analysis and querying")
templates = Jinja2Templates(directory="app/templates")

# Mount static files
app.mount("/static", StaticFiles(directory="app/static"), name="static")

app.include_router(upload_router)
app.include_router(query_router)

@app.on_event("startup")
async def startup_event():
    logger.info("Starting up application")
    # Create database tables with retry logic
    max_retries = 10
    retry_delay = 2

    for attempt in range(max_retries):
        try:
            logger.info("Database URL: %s", DATABASE_URL)
            logger.info("Attempting to create database tables (attempt %s/%s)...", attempt + 1, max_retries)
            await create_tables()
            logger.info("Database tables created successfully")
            return
        except Exception as e:
            logger.error("Error creating database tables (attempt %s): %s", attempt + 1, e)
            if attempt < max_retries - 1:
                logger.info("Retrying in %s seconds...", retry_delay)
                await asyncio.sleep(retry_delay)
            else:
                logger.error("Failed to create database tables after all retries")
                import traceback
                traceback.print_exc()

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})
