CREATE TABLE IF NOT EXISTS STAGED.ASTRONAUTS AS
SELECT
    _record_id AS id,
    name,
    agency_name AS agency,
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
FROM RAW.ASTRONAUTS;

CREATE TABLE IF NOT EXISTS STAGED.NASA_APOD AS
SELECT
    date,
    title,
    explanation,
    url,
    hdurl
FROM RAW.NASA_APOD;

CREATE TABLE IF NOT EXISTS STAGED.NASA_DONKI AS
SELECT
    messageID,
    messageType,
    messageIssueTime,
    messageBody
FROM RAW.NASA_DONKI;

CREATE TABLE IF NOT EXISTS STAGED.NASA_EXOPLANETS AS
SELECT
    pl_name AS planet_name,
    disc_year AS discovery_year,
    pl_controv_flag AS controversial_flag,
    discoverymethod AS discovery_method,
    disc_facility AS discovery_facility,
    disc_instrument AS discovery_instrument,
    pl_orbper AS orbital_period_days,
    pl_rade AS radius_earth_radii,
    st_rad AS star_radius_solar_radii,
    pl_orbsmax AS orbital_semi_major_axis_in_au -- the average distance the star and planet are apart in astronomical units (how far the sun is from earth)
FROM RAW.NASA_EXOPLANETS;