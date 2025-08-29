SELECT
    * -- intentionally using wildcard due to daily changing schema with 1 row return
FROM STAGED.NASA_APOD
WHERE title IS NULL;