CREATE OR REPLACE TABLE CLEANED.ASTRONAUTS AS
SELECT
    id,
    name,
    agency,
    agency_abbrev,
    image_url,
    time_in_space,
    eva_time,
    age,
    bio,
    wiki,
    spacewalks_count,
    url,
    thumbnail_url
FROM STAGED.ASTRONAUTS;