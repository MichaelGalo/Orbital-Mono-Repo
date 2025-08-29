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
FROM STAGED.ASTRONAUTS
WHERE name IS NULL AND agency IS NULL;