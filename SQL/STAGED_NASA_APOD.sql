CREATE TABLE IF NOT EXISTS STAGED.NASA_APOD AS
SELECT
    * -- intentionally using wildcard due to daily changing schema return
FROM RAW.NASA_APOD;