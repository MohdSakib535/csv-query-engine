from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from app.routes.upload import router as upload_router
from app.routes.query import router as query_router

app = FastAPI(title="CSV Q&A Analytics", description="AI-powered CSV data analysis and querying")
templates = Jinja2Templates(directory="app/templates")

# Mount static files
app.mount("/static", StaticFiles(directory="app/static"), name="static")

app.include_router(upload_router)
app.include_router(query_router)

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})