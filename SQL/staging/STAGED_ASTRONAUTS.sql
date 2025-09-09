CREATE OR REPLACE TABLE STAGED.ASTRONAUTS AS
SELECT
    _record_id AS id,
    name,
    agency_name AS agency,
    agency_abbrev,
    image_url,
    time_in_space_human_readable AS time_in_space,
    eva_time_human_readable AS eva_time,
    age,
    bio,
    wiki,
    spacewalks_count,
    url,
    thumbnail_url
FROM RAW_DATA.ASTRONAUTS
WHERE name IS NOT NULL;