CREATE OR REPLACE TABLE STAGED.NASA_APOD AS
SELECT
    * -- intentionally using wildcard due to daily changing columns schema
FROM RAW.NASA_APOD;