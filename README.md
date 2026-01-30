# CSV Q&A Analytics

A production-grade web application for analyzing CSV data using natural language queries, powered by DuckDB and optional OpenAI integration.

## Features

- Upload CSV files and automatically profile columns
- Ask natural language questions about your data
- **Rule-based SQL generation** (works without API key)
- **AI-powered SQL generation** using OpenAI GPT (set `OPENAI_API_KEY`)
- Safe SQL execution with DuckDB
- Interactive results with tables and charts
- Docker containerization for easy deployment

## Setup

1. Clone the repository
2. Set `OPENAI_API_KEY` in `.env` (required for AI SQL generation)
   - Optional: set `OPENAI_MODEL` (default: `gpt-4o-mini`)
3. For development: `docker-compose up --build` (auto-reloads on changes)
4. For local development without Docker: `pip install -r requirements.txt` then `uvicorn app:app --reload`

## Docker

For development with auto-reload on code changes:

```bash
docker-compose up --build
```

For production deployment:

```bash
docker build -t csv-qa-analytics .
docker run -p 8000:8000 --env-file .env csv-qa-analytics
```

## Usage

1. Open http://localhost:8000
2. Upload a CSV file
3. Ask a question like "Which services were affected by incidents in Mumbai last month?"
4. View the generated SQL, results table, and chart

## Security

- Only SELECT queries are allowed
- SQL validation prevents dangerous operations
- Row limits enforced for performance
