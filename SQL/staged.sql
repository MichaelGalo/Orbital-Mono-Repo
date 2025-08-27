CREATE TABLE IF NOT EXISTS STAGED.ASTRONAUTS AS
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
FROM RAW.ASTRONAUTS;

CREATE TABLE IF NOT EXISTS STAGED.NASA_APOD AS
SELECT
    *
FROM RAW.NASA_APOD;

--FIXME:
-- CREATE TABLE IF NOT EXISTS STAGED.NASA_APOD AS
-- SELECT
--     _record_id as id,
--     title,
--     date,
--     explanation,
--     url,
--     hdurl,
--     thumbnail_url,
--     media_type,
--     copyright,
--     concepts,
--     concept_tags
-- FROM RAW.NASA_APOD;

CREATE TABLE IF NOT EXISTS STAGED.NASA_DONKI AS
SELECT
    messageID,
    messageType,
    STRFTIME(STRPTIME(messageIssueTime, '%Y-%m-%dT%H:%MZ'), '%B %d, %Y %H:%M UTC') AS message_issue_time_human_readable,
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
    pl_orbsmax AS orbital_semi_major_axis_in_au
FROM RAW.NASA_EXOPLANETS;