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
WHERE date IS NOT NULL
    AND title IS NOT NULL
    AND url IS NOT NULL
    AND explanation IS NOT NULL;