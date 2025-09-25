import os
import pandas as pd
from sqlalchemy import create_engine
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

df = pd.read_csv(csv_path, nrows=10)


table_cols = [
    "book_id","goodreads_book_id","best_book_id","work_id","books_count",
    "isbn","isbn13","authors","original_publication_year","original_title",
    "title","language_code","average_rating","ratings_count",
    "work_ratings_count","work_text_reviews_count"
]


if "goodreads_book_id" not in df.columns:
    df["goodreads_book_id"] = None

# беремо тільки ті колонки, що в таблиці
df = df[table_cols]

with engine.begin() as conn:
    df.to_sql(
        name="books",
        schema="dwh",
        con=conn,
        if_exists="append",
        index=False
    )

print("✅ Loaded first 10 rows into dwh.books")