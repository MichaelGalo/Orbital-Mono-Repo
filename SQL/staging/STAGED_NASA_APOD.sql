CREATE OR REPLACE TABLE STAGED.NASA_APOD AS
SELECT
    * -- intentionally using wildcard due to daily changing columns schema
FROM RAW.NASA_APOD;

-- resource, concept_tags, title, date, url, hdurl, media_type, explanation, concepts, thumbnail_url, service_version, copyright