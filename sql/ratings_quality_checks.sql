-- =========================================
-- Data Quality Checks for dwh.fact_rating
-- =========================================

-- 1) Row count
SELECT COUNT(*) AS fact_rows FROM dwh.fact_rating;

-- 2) NULL checks
SELECT
  COUNT(*) FILTER (WHERE user_id IS NULL)  AS null_user_id,
  COUNT(*) FILTER (WHERE book_id IS NULL)  AS null_book_id,
  COUNT(*) FILTER (WHERE rating  IS NULL)  AS null_rating
FROM dwh.fact_rating;

-- 3) Rating range check (expected: 1..5)
SELECT MIN(rating) AS min_rating, MAX(rating) AS max_rating
FROM dwh.fact_rating;

-- 4) Primary key uniqueness (user_id, book_id)
-- (Should be 0; table PK should guarantee uniqueness)
SELECT user_id, book_id, COUNT(*) AS dup_cnt
FROM dwh.fact_rating
GROUP BY user_id, book_id
HAVING COUNT(*) > 1
LIMIT 50;

-- 5) Referential integrity to books (how many match)
-- NOTE: we don't enforce FK yet; this is an informational check
SELECT
  (SELECT COUNT(*) FROM dwh.fact_rating)                       AS total_ratings,
  (SELECT COUNT(*) FROM dwh.fact_rating fr
     JOIN dwh.books b ON b.book_id = fr.book_id)              AS matched_book_ids;

-- 6) Ratings distribution sanity (counts per 1..5)
SELECT rating, COUNT(*) AS cnt
FROM dwh.fact_rating
GROUP BY rating
ORDER BY rating;

-- 7) Out-of-range or invalid values (should be empty due to CHECK)
SELECT *
FROM dwh.fact_rating
WHERE rating < 1 OR rating > 5
LIMIT 50;

-- 8) Most active users (smoke-test data shape)
SELECT user_id, COUNT(*) AS ratings_given
FROM dwh.fact_rating
GROUP BY user_id
ORDER BY ratings_given DESC
LIMIT 10;

-- 9) Top books by received ratings (smoke-test data shape)
SELECT b.book_id, b.title, COUNT(*) AS ratings_count
FROM dwh.fact_rating fr
JOIN dwh.books b ON b.book_id = fr.book_id
GROUP BY b.book_id, b.title
ORDER BY ratings_count DESC
LIMIT 10;