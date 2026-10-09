/* Sample queries for the curated lakehouse tables.

   Table and column names are identical in catalog.json, the curated.db
   SQLite mirror, and the Glue Data Catalog, so every query below runs
   as is in both places: paste into sqlite3 against curated.db locally,
   or run in Amazon Athena against the Glue tables in production. */

/* 1. Daily event volume by event type */
SELECT event_date, event_type, COUNT(*) AS events
FROM clickstream_events
GROUP BY event_date, event_type
ORDER BY event_date, event_type;

/* 2. Purchase conversion rate by region */
SELECT region,
       COUNT(*) AS total_events,
       SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchases,
       ROUND(100.0 * SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END)
             / NULLIF(COUNT(*), 0), 2) AS conversion_pct
FROM clickstream_events
GROUP BY region
ORDER BY conversion_pct DESC;

/* 3. Top pages by views */
SELECT page_url, COUNT(*) AS views
FROM clickstream_events
WHERE event_type = 'page_view'
GROUP BY page_url
ORDER BY views DESC
LIMIT 10;

/* 4. Product dimension snapshot by category */
SELECT category,
       COUNT(*) AS products,
       ROUND(AVG(price), 2) AS avg_price,
       ROUND(MAX(price), 2) AS max_price
FROM product_dim
GROUP BY category
ORDER BY products DESC;

/* 5. Most recently updated products, proving the SCD1 merge */
SELECT product_id, product_name, price, updated_at
FROM product_dim
ORDER BY updated_at DESC, product_id
LIMIT 10;
