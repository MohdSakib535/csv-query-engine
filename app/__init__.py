from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from app.routes.upload import router as upload_router
from app.routes.query import router as query_router

app = FastAPI()
templates = Jinja2Templates(directory="app/templates")

app.include_router(upload_router)
app.include_router(query_router)

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})