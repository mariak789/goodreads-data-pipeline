-- =========================================
-- Analytics queries for dwh.fact_rating
-- =========================================

-- A1) Top-10 books by number of ratings
SELECT 
    b.book_id,
    b.title,
    b.authors,
    COUNT(r.rating) AS ratings_count,
    ROUND(AVG(r.rating), 2) AS avg_rating
FROM dwh.fact_rating r
JOIN dwh.books b ON b.book_id = r.book_id
GROUP BY b.book_id, b.title, b.authors
ORDER BY ratings_count DESC
LIMIT 10;

-- A2) Top-10 most active users (by number of ratings submitted)
SELECT 
    r.user_id,
    COUNT(r.rating) AS ratings_given,
    ROUND(AVG(r.rating), 2) AS avg_rating_given
FROM dwh.fact_rating r
GROUP BY r.user_id
ORDER BY ratings_given DESC
LIMIT 10;

-- A3) Rating distribution (histogram 1–5)
SELECT 
    r.rating,
    COUNT(*) AS ratings_count
FROM dwh.fact_rating r
GROUP BY r.rating
ORDER BY r.rating;