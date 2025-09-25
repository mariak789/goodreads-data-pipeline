# pipelines/load_books_full.py
import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
host = os.getenv("PG_HOST", "localhost")
port = os.getenv("PG_PORT", "5432")
db   = os.getenv("PG_DB", "goodbooks")
user = os.getenv("PG_USER", "maria")
pwd  = os.getenv("PG_PASSWORD", "maria_password")

url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
engine = create_engine(url)

csv_path = "data/raw/books.csv"

# Колонки, які треба підтягнути з CSV
cols = [
    "id", "book_id","goodreads_book_id","best_book_id","work_id","books_count",
    "isbn","isbn13","authors","original_publication_year","original_title",
    "title","language_code","average_rating","ratings_count",
    "work_ratings_count","work_text_reviews_count"
]

# 1) Почистимо staging
with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE dwh.books_stg"))

# dtype map (уникаємо float для ідентифікаторів)
dtype_map = {
    "id": "Int64",
    "book_id": "Int64",
    "best_book_id": "Int64",
    "work_id": "Int64",
    "books_count": "Int64",
    "original_publication_year": "Int64",
    "ratings_count": "Int64",
    "work_ratings_count": "Int64",
    "work_text_reviews_count": "Int64",
    "average_rating": "float",
    "isbn": "string",
    "isbn13": "string",
}

reader = pd.read_csv(csv_path, chunksize=50_000, dtype=dtype_map)

total_rows = 0
for chunk in reader:
    if "goodreads_book_id" not in chunk.columns:
        chunk["goodreads_book_id"] = None

    # перейменуємо "id" → "source_book_id"
    chunk = chunk.rename(columns={"id": "source_book_id"})

    # залишаємо потрібні колонки
    chunk = chunk[["source_book_id"] + cols[1:]]  # ставимо source_book_id першим

    # нормалізуємо текстові поля
    for c in ["isbn", "isbn13", "authors", "original_title", "title", "language_code"]:
        if c in chunk.columns:
            chunk[c] = chunk[c].astype("string").where(~chunk[c].isna(), None)

    # пишемо в staging
    with engine.begin() as conn:
        chunk.to_sql(
            name="books_stg",
            schema="dwh",
            con=conn,
            if_exists="append",
            index=False,
        )
    total_rows += len(chunk)

print(f"Staged {total_rows} rows into dwh.books_stg")

# 3) Merge у основну таблицю
merge_sql = """
INSERT INTO dwh.books (
  source_book_id, book_id, goodreads_book_id, best_book_id, work_id, books_count,
  isbn, isbn13, authors, original_publication_year, original_title,
  title, language_code, average_rating, ratings_count,
  work_ratings_count, work_text_reviews_count
)
SELECT
  s.source_book_id, s.book_id, s.goodreads_book_id, s.best_book_id, s.work_id, s.books_count,
  s.isbn, s.isbn13, s.authors, s.original_publication_year, s.original_title,
  s.title, s.language_code, s.average_rating, s.ratings_count,
  s.work_ratings_count, s.work_text_reviews_count
FROM dwh.books_stg s
ON CONFLICT (book_id) DO UPDATE
SET source_book_id = EXCLUDED.source_book_id;  -- оновлюємо source_book_id
"""

with engine.begin() as conn:
    conn.execute(text(merge_sql))
    count = conn.execute(text("SELECT COUNT(*) FROM dwh.books")).scalar_one()

print(f"✅ Merge done. dwh.books now has {count} rows.")