CREATE OR REPLACE TABLE CLEANED.NASA_APOD AS
SELECT
    title,
    explanation,
    date,
    url,
    hdurl,
    thumbnail_url,
    copyright
FROM STAGED.NASA_APOD;