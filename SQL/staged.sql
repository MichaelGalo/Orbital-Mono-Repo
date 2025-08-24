CREATE TABLE IF NOT EXISTS
SELECT 
    _record_id AS id,
    name, 
    agency,
    image,
    time_in_space, 
    eva_time, 
    age, 
    bio, 
    wiki, 
    spacewalks_count,
FROM RAW.ASTRONAUTS