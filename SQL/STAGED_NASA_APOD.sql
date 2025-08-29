CREATE TABLE IF NOT EXISTS STAGED.NASA_APOD AS
SELECT
    * -- intentionally using wildcard due to daily changing schema with 1 row return
FROM RAW.NASA_APOD;