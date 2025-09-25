-- =========================================
-- Analytics queries for dwh.fact_rating
-- Uses correct join key: books.source_book_id = fact_rating.book_id
-- =========================================

-- A1) Top-10 books by number of ratings
SELECT 
    b.book_id,                          -- big Goodreads ID
    b.source_book_id,                   -- small ID (1..10000)
    b.title,
    b.authors,
    COUNT(fr.rating) AS ratings_count,
    ROUND(AVG(fr.rating), 2) AS avg_rating
FROM dwh.fact_rating fr
JOIN dwh.books b
  ON b.source_book_id = fr.book_id
GROUP BY b.book_id, b.source_book_id, b.title, b.authors
ORDER BY ratings_count DESC
LIMIT 10;

-- A2) Top-10 most active users (by number of ratings submitted)
SELECT 
    fr.user_id,
    COUNT(*) AS ratings_given,
    ROUND(AVG(fr.rating), 2) AS avg_rating_given
FROM dwh.fact_rating fr
GROUP BY fr.user_id
ORDER BY ratings_given DESC
LIMIT 10;

-- A3) Rating distribution (histogram 1–5)
SELECT 
    fr.rating,
    COUNT(*) AS ratings_count
FROM dwh.fact_rating fr
GROUP BY fr.rating
ORDER BY fr.rating;

-- A4) Top-100 books by Bayesian score (C=m prior weight)
WITH params AS (
  SELECT 4.0::numeric AS m, 5000::int AS C
),
book_agg AS (
  SELECT 
      b.book_id,
      b.source_book_id,
      b.title,
      b.authors,
      COUNT(*)::bigint AS n,              -- number of ratings
      AVG(fr.rating)::numeric(4,2) AS r   -- average rating from facts
  FROM dwh.fact_rating fr
  JOIN dwh.books b ON b.source_book_id = fr.book_id
  GROUP BY b.book_id, b.source_book_id, b.title, b.authors
)
SELECT
  book_id,
  source_book_id,
  title,
  authors,
  n AS ratings_count,
  r AS fact_avg_rating,
  ROUND( ((p.C * p.m) + (ba.n * ba.r)) / NULLIF((p.C + ba.n),0), 3 ) AS bayesian_score
FROM book_agg ba
CROSS JOIN params p
ORDER BY bayesian_score DESC, ratings_count DESC
LIMIT 100;