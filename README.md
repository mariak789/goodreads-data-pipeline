# Goodreads Data Pipeline (Goodbooks-10k)

Personal data-engineering project to practice **ETL → DWH → Analytics → Visualization** on the
[Goodbooks-10k](https://www.kaggle.com/datasets/zygmunt/goodbooks-10k) dataset.

The goal is to build a small data warehouse (DWH) on PostgreSQL, run quality checks and perform first analytical queries and visualizations (via Jupyter Notebook)

## Tech Stack
- Python 3.13
- PosgtreSQL (Docker)
- SQLAlchemy + Pandas
- Jupyter Notebook (Malplotlib for visualizations)

## Setup & Usage

1. Start PostgreSQL with Docker:
```bash
docker run --name goodbooks_postgres \
    -e POSTGRES_PASSWORD=postgres \
    -p 5432:5432 -d postgres:15
```
2. Configure environment variables, use the provided .env.example as a template
```bash
cp .env.example .env
```

3. Edit .env with your PostgreSQL credentials

PG_HOST=localhost
PG_PORT=5432
PG_DB=goodbooks
PG_USER=your_username
PG_PASSWORD=your_password

4. Create role & database:

Connect to PostgreSQL
```bash
psql -h localhost -U postgres
```

Inside psql:
```bash
CREATE ROLE your_username LOGIN PASSWORD 'your_password' SUPERUSER;
CREATE DATABASE goodbooks OWNER your_username;
```

5. Initialize tables
```bash
psql -h localhost -U your_username -d goodbooks -f sql/create_books.sql
psql -h localhost -U your_username -d goodbooks -f sql/create_ratings.sql
```

6. Load data with ETL pipelines
```bash
python pipelines/load_books_full.py
python pipelines/load_ratings_full.py
```

7. Run data quality checks 
```bash
psql -h localhost -U your_username -d goodbooks -f sql/ratings_quality_checks.sql
psql -h localhost -U your_username -d goodbooks -f sql/quality_checks.sql
```

## ✅ Data Quality Tests
- rating must be between 1 and 5
- user_id and book_id are NOT NULL
- no duplicate (user_id, book_id) pairs

# Analytics

In Jupyter Notebook (notebooks/exploration.ipynb) you can explore:
- Top-10 books by number of ratings
- Top-10 most active users
- Rating distribution (histogram 1–5)

All charts can be exported into notebooks/exports/.

## Artifacts
Database: goodbooks with two core tables:
- dwh.books (dimension table)
- dwh.fact_rating (fact table with ~1M rows)
- quality checks passed
- analytical queries and dashboards created in Jupyter