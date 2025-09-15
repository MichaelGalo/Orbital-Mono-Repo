CREATE OR REPLACE TABLE STAGED.NASA_APOD AS
SELECT
    title,
    explanation,
    date,
    url,
    hdurl,
    thumbnail_url,
    copyright
FROM RAW_DATA.NASA_APOD
WHERE title IS NOT NULL;