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

# Колонки, з якими працюємо в DWH
cols = [
    "book_id","goodreads_book_id","best_book_id","work_id","books_count",
    "isbn","isbn13","authors","original_publication_year","original_title",
    "title","language_code","average_rating","ratings_count",
    "work_ratings_count","work_text_reviews_count"
]

# 1) Почистимо staging (TRUNCATE)
with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE dwh.books_stg"))

# 2) Читаємо csv порціями і пишемо в staging
# ВАЖЛИВО: isbn / isbn13 читаємо як string, щоб не було 978... -> 9.78e+12 чи "....0"
dtype_map = {
    "isbn": "string",
    "isbn13": "string",
    "book_id": "Int64",
    "best_book_id": "Int64",
    "work_id": "Int64",
    "books_count": "Int64",
    "original_publication_year": "Int64",
    "ratings_count": "Int64",
    "work_ratings_count": "Int64",
    "work_text_reviews_count": "Int64",
    "average_rating": "float",
}

reader = pd.read_csv(csv_path, chunksize=50_000, dtype=dtype_map)

total_rows = 0
for chunk in reader:
    # fallback для goodreads_book_id, якщо його немає у файлі
    if "goodreads_book_id" not in chunk.columns:
        chunk["goodreads_book_id"] = None

    # залишаємо тільки потрібні поля
    chunk = chunk[cols]

    # нормалізуємо типи для текстових колонок isbn*/authors/title
    for c in ["isbn", "isbn13", "authors", "original_title", "title", "language_code"]:
        if c in chunk.columns:
            chunk[c] = chunk[c].astype("string").where(~chunk[c].isna(), None)

    # write into staging
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

# 3) Merge into main table
merge_sql = """
INSERT INTO dwh.books (
  book_id, goodreads_book_id, best_book_id, work_id, books_count,
  isbn, isbn13, authors, original_publication_year, original_title,
  title, language_code, average_rating, ratings_count,
  work_ratings_count, work_text_reviews_count
)
SELECT
  s.book_id, s.goodreads_book_id, s.best_book_id, s.work_id, s.books_count,
  s.isbn, s.isbn13, s.authors, s.original_publication_year, s.original_title,
  s.title, s.language_code, s.average_rating, s.ratings_count,
  s.work_ratings_count, s.work_text_reviews_count
FROM dwh.books_stg s
ON CONFLICT (book_id) DO NOTHING;  -- ідемпотентність
"""

with engine.begin() as conn:
    result = conn.execute(text(merge_sql))
    # SQLAlchemy не завжди повертає inserted rows для INSERT..SELECT,
    # тому просто перевіримо підсумок окремим запитом:
    count = conn.execute(text("SELECT COUNT(*) FROM dwh.books")).scalar_one()

print(f"✅ Merge done. dwh.books now has {count} rows.")