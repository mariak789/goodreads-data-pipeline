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

csv_path = "data/raw/ratings.csv"

# Read in chunks to keep memory low
# Goodbooks-10k has ~6M rows; 250k per chunk is a good start
chunksize = 250_000

# dtypes help avoid unexpected float parsing
dtype_map = {
    "user_id": "Int64",
    "book_id": "Int64",
    "rating": "Int8",
}

loaded_rows = 0
dropped_nulls = 0
dropped_out_of_range = 0

# 1) clean staging
with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE dwh.ratings_stg"))

reader = pd.read_csv(csv_path, chunksize=chunksize, dtype=dtype_map)

for i, chunk in enumerate(reader, start=1):
    # Basic quality filters

    # drop nulls
    before = len(chunk)
    chunk = chunk.dropna(subset=["user_id", "book_id", "rating"])
    dropped_nulls += before - len(chunk)

    # keep only 1..5
    mask = (chunk["rating"] >= 1) & (chunk["rating"] <= 5)
    dropped_out_of_range += len(chunk) - mask.sum()
    chunk = chunk[mask]

    # cast to plain ints for DB insertion
    chunk["user_id"] = chunk["user_id"].astype(int)
    chunk["book_id"] = chunk["book_id"].astype(int)
    chunk["rating"]  = chunk["rating"].astype(int)

    # write to staging
    with engine.begin() as conn:
        chunk.to_sql(
            name="ratings_stg",
            schema="dwh",
            con=conn,
            if_exists="append",
            index=False,
        )
    loaded_rows += len(chunk)
    print(f"[chunk {i}] staged: {len(chunk):,} rows")

print(f"Staging complete. Rows staged: {loaded_rows:,}. "
      f"Dropped nulls: {dropped_nulls:,}. "
      f"Dropped out-of-range: {dropped_out_of_range:,}.")

# 2) idempotent merge into fact table
merge_sql = """
INSERT INTO dwh.fact_rating (user_id, book_id, rating)
SELECT user_id, book_id, MAX(rating) AS rating
FROM dwh.ratings_stg
GROUP BY user_id, book_id
ON CONFLICT (user_id, book_id) DO UPDATE
SET rating = EXCLUDED.rating;
"""

with engine.begin() as conn:
    conn.execute(text(merge_sql))
    total = conn.execute(text("SELECT COUNT(*) FROM dwh.fact_rating")).scalar_one()

print(f"✅ Merge done. dwh.fact_rating now has {total:,} rows.")