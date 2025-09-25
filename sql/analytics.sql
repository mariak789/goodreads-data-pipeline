-- =========================================
-- Analytics queries for dwh.books (Goodbooks-10k)
-- All queries can be executed independently
-- =========================================

-- Parameters for Bayesian average:
-- m  = prior_mean (average rating across all books, ~3.9–4.1)
-- C  = prior_weight (strength of the prior, e.g. 5000 votes)
WITH params AS (
  SELECT 4.0::numeric AS m, 5000::int AS C
)

-- A1) Top-100 books by Bayesian average score
SELECT
  b.book_id,
  b.title,
  b.authors,
  b.average_rating,
  b.ratings_count,
  ROUND(((p.C * p.m) + (b.ratings_count * b.average_rating)) / NULLIF((p.C + b.ratings_count),0), 3) AS bayesian_score
FROM dwh.books b
CROSS JOIN params p
WHERE b.ratings_count IS NOT NULL
ORDER BY bayesian_score DESC, b.ratings_count DESC
LIMIT 100;

-- A2) Most popular authors by total number of ratings
SELECT
  split_part(b.authors, ',', 1) AS main_author,  -- take the first author if multiple
  SUM(b.ratings_count) AS total_ratings,
  ROUND(AVG(b.average_rating), 2) AS avg_rating,
  COUNT(*) AS books_count
FROM dwh.books b
WHERE b.authors IS NOT NULL
GROUP BY main_author
ORDER BY total_ratings DESC
LIMIT 20;

-- A3) Rating distribution (histogram)
SELECT
  FLOOR(average_rating)::int AS rating_bucket,
  COUNT(*) AS books_count
FROM dwh.books
WHERE average_rating IS NOT NULL
GROUP BY rating_bucket
ORDER BY rating_bucket;

-- A4) Average rating trend by publication year
SELECT
  original_publication_year,
  ROUND(AVG(average_rating), 2) AS avg_rating,
  COUNT(*) AS books_published
FROM dwh.books
WHERE original_publication_year IS NOT NULL
GROUP BY original_publication_year
HAVING COUNT(*) >= 5   -- only years with at least 5 books
ORDER BY original_publication_year;