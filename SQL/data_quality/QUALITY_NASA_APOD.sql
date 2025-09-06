SELECT
    title,
    explanation,
    date,
    url,
    hdurl,
    thumbnail_url,
    copyright
FROM STAGED.NASA_APOD
WHERE title IS NULL
    OR explanation IS NULL
    OR url IS NULL;