-- Ensure schema (owned by your local user)
CREATE SCHEMA IF NOT EXISTS dwh AUTHORIZATION maria;

-- Staging table
CREATE TABLE IF NOT EXISTS dwh.ratings_stg (
  user_id  INT,
  book_id  INT,
  rating   INT
);

-- Fact table (upsert key)
CREATE TABLE IF NOT EXISTS dwh.fact_rating (
  user_id  INT NOT NULL,
  book_id  INT NOT NULL,
  rating   SMALLINT NOT NULL CHECK (rating BETWEEN 1 AND 5),
  PRIMARY KEY (user_id, book_id)
);

-- Indexes to speed up joins and lookups
CREATE INDEX IF NOT EXISTS fact_rating_book_idx ON dwh.fact_rating (book_id);
CREATE INDEX IF NOT EXISTS fact_rating_user_idx ON dwh.fact_rating (user_id);