-- ==========================
-- Data Quality Checks
-- Target table: dwh.books
-- ==========================

-- 1. Amount of books without name
SELECT COUNT(*) AS missing_titles
FROM dwh.books
WHERE title IS NULL OR title = '';

-- 2. book_id dublicates (not allowed)
SELECT book_id, COUNT(*)
FROM dwh.books
GROUP BY book_id
HAVING COUNT(*) > 1;

-- 3. Min and max rating
SELECT MIN(average_rating) AS min_rating,
       MAX(average_rating) AS max_rating
FROM dwh.books;

-- 4. Amount of books without author
SELECT COUNT(*) AS missing_authors
FROM dwh.books
WHERE authors IS NULL OR authors = '';

-- ==========================
-- Basic Analytics
-- ==========================

-- A1. Top 10 books via average rating (minimum 5000 evaluations)
SELECT title, authors, average_rating, ratings_count
FROM dwh.books
WHERE ratings_count >= 5000
ORDER BY average_rating DESC, ratings_count DESC
LIMIT 10;

-- A2. Top-10 popular books by amount of evaluations
SELECT title, authors, ratings_count, average_rating
FROM dwh.books
ORDER BY ratings_count DESC
LIMIT 10;

-- A3. Average rating by year of publications
SELECT original_publication_year,
       ROUND(AVG(average_rating), 2) AS avg_rating,
       COUNT(*) AS books_published
FROM dwh.books
WHERE original_publication_year IS NOT NULL
GROUP BY original_publication_year
ORDER BY original_publication_year
LIMIT 20;