CREATE TABLE IF NOT EXISTS
SELECT 
    _record_id AS id,
    name, 
    agency_name,
    agency_abbrev,
    image,
    time_in_space, 
    eva_time, 
    age, 
    bio, 
    wiki, 
    spacewalks_count,
    url,
    thumbnail_url
FROM RAW.ASTRONAUTS