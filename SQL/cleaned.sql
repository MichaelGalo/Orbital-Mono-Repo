CREATE TABLE IF NOT EXISTS CLEANED.ASTRONAUTS AS
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

CREATE TABLE IF NOT EXISTS CLEANED.NASA_APOD AS
SELECT
    date,
    title,
    explanation,
    url,
    hdurl
FROM STAGED.NASA_APOD;

CREATE TABLE IF NOT EXISTS CLEANED.NASA_DONKI AS
SELECT
    messageID AS message_id,
    messageType AS message_type,
    message_issue_time_human_readable AS message_issue_time,
    messageBody
FROM STAGED.NASA_DONKI;

CREATE TABLE IF NOT EXISTS CLEANED.NASA_EXOPLANETS AS
SELECT
    planet_name,
    discovery_year,
    controversial_flag,
    discovery_method,
    discovery_facility,
    discovery_instrument,
    orbital_period_days,
    radius_earth_radii,
    star_radius_solar_radii,
    orbital_semi_major_axis_in_au
FROM STAGED.NASA_EXOPLANETS;